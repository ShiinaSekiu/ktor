/*
 * Copyright 2014-2026 JetBrains s.r.o and contributors. Use of this source code is governed by the Apache 2.0 license.
 */

package io.ktor.tests.server.netty

import io.ktor.server.application.*
import io.ktor.server.http.HttpRequestLifecycle
import io.ktor.server.netty.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import io.ktor.server.test.base.*
import io.ktor.utils.io.*
import io.netty.bootstrap.Bootstrap
import io.netty.channel.*
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioDatagramChannel
import io.netty.handler.codec.http3.*
import io.netty.handler.codec.quic.*
import kotlinx.coroutines.*
import kotlinx.coroutines.CancellationException
import java.net.InetSocketAddress
import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.system.measureTimeMillis
import kotlin.test.Test
import kotlin.test.assertTrue

/**
 * Reproducers for HTTP/3 engine issues. Both tests assert the DESIRED behavior and
 * therefore FAIL on current main:
 *
 * Issue 1: user handler code is dispatched on the QUIC event-loop thread.
 *   [NettyHttp3Handler.startHttp3] builds the call context with the single-argument
 *   `NettyDispatcher.CurrentContext(context)` (defaulting to `context.executor()`) and the handler
 *   never receives the `callEventGroup`, unlike HTTP/1 and HTTP/2 which use `pinnedCallExecutor`
 *   (KTOR-9542). Since all QUIC connections of a connector share one DatagramChannel driven by a
 *   single event loop, any blocking user code freezes the entire HTTP/3 listener, including QUIC
 *   handshakes of unrelated new connections.
 *
 * Issue 2: `HttpRequestCloseHandlerKey` is never invoked for HTTP/3 streams.
 *   [NettyHttp3Handler.channelInactive] only cancels the handler job, so
 *   `HttpRequestLifecycle(cancelCallOnClose = true)` — verified for HTTP/1/2 by
 *   [HttpRequestLifecycleTest] — is silently a no-op on HTTP/3.
 *
 * (Issue 3, observe while running: aborting the QUIC connection mid-request in the second test
 *  logs "HTTP/3 stream exception" with a full stack trace at ERROR level from
 *  [NettyHttp3Handler.exceptionCaught] — client-triggerable log spam; HTTP/1 logs client
 *  disconnects at TRACE since KTOR-646, HTTP/2 closes silently.)
 */
class NettyHttp3IssuesReproTest :
    EngineTestBase<NettyApplicationEngine, NettyApplicationEngine.Configuration>(Netty) {

    init {
        enableSsl = true
        // Windows: FreePorts probes with TCP sockets, but HTTP/3 binds UDP on sslPort;
        // OS-assigned TCP ports frequently fall into Hyper-V/WSL UDP excluded port ranges,
        // failing the UDP bind. Pin ports outside the dynamic/excluded range for stability.
        port = 24430
        sslPort = 24431
    }

    @OptIn(ExperimentalKtorApi::class)
    override fun configure(configuration: NettyApplicationEngine.Configuration) {
        configuration.enableHttp3()
    }

    @Test
    fun `blocking user code on one HTTP3 connection must not stall other connections`() = runTest {
        createAndStartServer {
            application.routing {
                get("/thread") { call.respondText(Thread.currentThread().name) }
                get("/instant") { call.respondText("ok") }
                get("/block") {
                    Thread.sleep(1500) // deliberately blocking user code (e.g. JDBC, file IO)
                    call.respondText("done on " + Thread.currentThread().name)
                }
            }
        }

        // warmup (first QUIC connection pays JVM/native init costs)
        withHttp3Client { quic ->
            println("[repro] HTTP/3 handler thread: " + sendHttp3Request(quic, "GET", "/thread").body)
        }
        withHttp3Client { quic -> sendHttp3Request(quic, "GET", "/instant") }

        val baselineMs = measureTimeMillis {
            withHttp3Client { quic -> sendHttp3Request(quic, "GET", "/instant") }
        }
        println("[repro] fresh connection + GET /instant with idle server: ${baselineMs}ms")

        var stalledMs = 0L
        withHttp3Client { connectionA ->
            // fire GET /block on connection A without awaiting the response
            val pending = Http3ResponseHandler()
            val stream = Http3.newRequestStream(connectionA, pending).sync().getNow()
            val headers = DefaultHttp3Headers().apply {
                method("GET")
                path("/block")
                scheme("https")
                authority("localhost:$sslPort")
            }
            stream.writeAndFlush(DefaultHttp3HeadersFrame(headers)).sync()
            stream.shutdownOutput().sync()
            Thread.sleep(200) // let the server enter the blocking section

            // a brand-new QUIC connection (fresh handshake) on the same connector
            stalledMs = measureTimeMillis {
                withHttp3Client { connectionB -> sendHttp3Request(connectionB, "GET", "/instant") }
            }
            println("[repro] fresh connection + GET /instant while another connection's handler blocks: ${stalledMs}ms")

            pending.responseQueue.poll(5, TimeUnit.SECONDS) // drain /block response
        }

        assertTrue(
            stalledMs - baselineMs < 1000,
            "an unrelated new HTTP/3 connection was stalled (${stalledMs}ms vs ${baselineMs}ms baseline) " +
                "by blocking user code on another connection: user code runs on the shared QUIC event " +
                "loop (no pinnedCallExecutor/callEventGroup dispatch, unlike HTTP/1 and HTTP/2)"
        )
    }

    @Test
    fun `cancelCallOnClose cancels HTTP3 calls when the client disconnects`() = runTest {
        val cancelled = AtomicBoolean(false)
        val ranToCompletion = AtomicBoolean(false)
        val handlerFinished = CountDownLatch(1)

        createAndStartServer {
            application.install(HttpRequestLifecycle) {
                cancelCallOnClose = true
            }
            application.routing {
                get("/cancellable") {
                    try {
                        delay(2000)
                        ranToCompletion.set(true)
                        call.respondText("finished")
                    } catch (cause: CancellationException) {
                        // absorb and record, mirroring HttpRequestLifecycleTest's cancellableRoute
                        cancelled.set(true)
                    } finally {
                        handlerFinished.countDown()
                    }
                }
            }
        }

        // start the request over HTTP/3, then abruptly close the whole QUIC connection mid-handling
        val group = NioEventLoopGroup(1)
        try {
            val quicSslContext = QuicSslContextBuilder.forClient()
                .trustManager(io.netty.handler.ssl.util.InsecureTrustManagerFactory.INSTANCE)
                .applicationProtocols(*Http3.supportedApplicationProtocols())
                .build()
            val codec = Http3.newQuicClientCodecBuilder()
                .sslContext(quicSslContext)
                .maxIdleTimeout(30_000, TimeUnit.MILLISECONDS)
                .initialMaxData(10_000_000)
                .initialMaxStreamDataBidirectionalLocal(1_000_000)
                .build()
            val udpChannel = Bootstrap()
                .group(group)
                .channel(NioDatagramChannel::class.java)
                .handler(codec)
                .bind(0).sync().channel()
            val quicChannel = QuicChannel.newBootstrap(udpChannel)
                .handler(Http3ClientConnectionHandler())
                .remoteAddress(InetSocketAddress("127.0.0.1", sslPort))
                .connect().get()

            val stream = Http3.newRequestStream(quicChannel, Http3ResponseHandler()).sync().getNow()
            val headers = DefaultHttp3Headers().apply {
                method("GET")
                path("/cancellable")
                scheme("https")
                authority("localhost:$sslPort")
            }
            stream.writeAndFlush(DefaultHttp3HeadersFrame(headers)).sync()
            stream.shutdownOutput().sync()

            Thread.sleep(300) // handler is now inside delay(2000)
            quicChannel.close().sync() // client goes away mid-request
            udpChannel.close().sync()
        } finally {
            group.shutdownGracefully(0, 500, TimeUnit.MILLISECONDS)
        }

        assertTrue(handlerFinished.await(5, TimeUnit.SECONDS), "handler never finished")
        println("[repro] after client disconnect: cancelled=${cancelled.get()} ranToCompletion=${ranToCompletion.get()}")
        assertTrue(
            cancelled.get(),
            "HttpRequestLifecycle(cancelCallOnClose=true) did not cancel the call on client disconnect " +
                "over HTTP/3: NettyHttp3Handler.channelInactive never invokes HttpRequestCloseHandlerKey " +
                "(HTTP/1 and HTTP/2 do, see NettyHttp1Handler.onConnectionClose / NettyHttp2Handler.onStreamClose)"
        )
    }

    // --- HTTP/3 client helpers (copied from NettyHttp3Test, where they are private) ---

    private data class Http3Response(
        val status: String,
        val headers: Map<String, String>,
        val body: String
    )

    private class Http3ResponseHandler : ChannelInboundHandlerAdapter() {
        val responseQueue = LinkedBlockingQueue<Http3Response>()
        private var status: String = ""
        private var headers: MutableMap<String, String> = mutableMapOf()
        private val bodyParts = mutableListOf<ByteArray>()

        override fun channelRead(ctx: ChannelHandlerContext, msg: Any) {
            when (msg) {
                is Http3HeadersFrame -> {
                    val h = msg.headers()
                    status = h.status()?.toString() ?: ""
                    h.forEach { (name, value) ->
                        val nameStr = name.toString()
                        if (!nameStr.startsWith(":")) {
                            headers[nameStr] = value.toString()
                        }
                    }
                }

                is Http3DataFrame -> {
                    val content = msg.content()
                    val bytes = ByteArray(content.readableBytes())
                    content.readBytes(bytes)
                    bodyParts.add(bytes)
                    msg.release()
                }

                else -> super.channelRead(ctx, msg)
            }
        }

        override fun channelInactive(ctx: ChannelHandlerContext) {
            val body = bodyParts.joinToString("") { String(it, Charsets.UTF_8) }
            responseQueue.offer(Http3Response(status, headers, body))
            status = ""
            headers = mutableMapOf()
            bodyParts.clear()
            super.channelInactive(ctx)
        }
    }

    private suspend fun withHttp3Client(block: suspend (QuicChannel) -> Unit) {
        val group = NioEventLoopGroup(1)
        try {
            val quicSslContext = QuicSslContextBuilder.forClient()
                .trustManager(io.netty.handler.ssl.util.InsecureTrustManagerFactory.INSTANCE)
                .applicationProtocols(*Http3.supportedApplicationProtocols())
                .build()

            val quicClientCodec = Http3.newQuicClientCodecBuilder()
                .sslContext(quicSslContext)
                .maxIdleTimeout(30_000, TimeUnit.MILLISECONDS)
                .initialMaxData(10_000_000)
                .initialMaxStreamDataBidirectionalLocal(1_000_000)
                .initialMaxStreamDataBidirectionalRemote(1_000_000)
                .initialMaxStreamsBidirectional(100)
                .build()

            val udpChannel = Bootstrap()
                .group(group)
                .channel(NioDatagramChannel::class.java)
                .handler(quicClientCodec)
                .bind(0)
                .sync()
                .channel()

            val quicChannel = QuicChannel.newBootstrap(udpChannel)
                .handler(Http3ClientConnectionHandler())
                .remoteAddress(InetSocketAddress("127.0.0.1", sslPort))
                .connect()
                .get()

            try {
                block(quicChannel)
            } finally {
                quicChannel.close().sync()
                udpChannel.close().sync()
            }
        } finally {
            group.shutdownGracefully().sync()
        }
    }

    private fun sendHttp3Request(
        quicChannel: QuicChannel,
        method: String,
        path: String
    ): Http3Response {
        val responseHandler = Http3ResponseHandler()

        val stream = Http3.newRequestStream(quicChannel, responseHandler).sync().getNow()

        val headers = DefaultHttp3Headers().apply {
            method(method)
            path(path)
            scheme("https")
            authority("localhost:$sslPort")
        }
        stream.writeAndFlush(DefaultHttp3HeadersFrame(headers)).sync()
        stream.shutdownOutput().sync()

        return responseHandler.responseQueue.poll(10, TimeUnit.SECONDS)
            ?: error("Timed out waiting for HTTP/3 response")
    }
}
