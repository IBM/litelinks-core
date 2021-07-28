/*
 * Copyright 2021 IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.ibm.watson.litelinks.server;

import com.ibm.watson.litelinks.NettyCommon;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import static io.netty.handler.codec.http.HttpHeaderNames.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaderValues.KEEP_ALIVE;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * Used just for (Kubernetes) health probes
 */
class ProbeHttpServer implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(ProbeHttpServer.class);

    static final String READY_URI = "/ready", LIVE_URI = "/live";

    private final int port;

    private final BooleanSupplier readinessChecker;
    private final BooleanSupplier livenessChecker;

    protected final Channel serverChannel;

    private final AtomicBoolean ready;

    public ProbeHttpServer(int port, BooleanSupplier readinessChecker,
            BooleanSupplier livenessChecker, boolean ready) throws Exception {
        this.port = port;
        this.readinessChecker = readinessChecker;
        this.livenessChecker = livenessChecker;
        this.ready = new AtomicBoolean(ready);
        this.serverChannel = startServer();
    }

    public void setReady(boolean ready) {
        if (this.ready.compareAndSet(!ready, ready)) {
            logger.info("HTTP health probe state changed to " + (ready? "READY" : "NOT READY"));
        }
    }

    private Channel startServer() throws Exception {
        logger.info("Starting litelinks health probe http server on port " + port);
        ServerBootstrap b = new ServerBootstrap()
                .group(NettyTServer.getBossGroup(), NettyCommon.getWorkerGroup())
                .channel(NettyTServer.getServerChannelClass())
//              .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new HttpRequestDecoder()).addLast(new HttpResponseEncoder());
                        p.addLast(new SimpleChannelInboundHandler<HttpRequest>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, HttpRequest req) {
                                boolean keepAlive = HttpUtil.isKeepAlive(req);
                                String uri = req.uri();
                                if (uri.endsWith("/")) {
                                    uri = uri.substring(0, uri.length() - 1);
                                }
                                HttpResponseStatus status = HttpResponseStatus.NOT_FOUND;
                                if (READY_URI.equals(uri)) {
                                    status = ready.get() && (readinessChecker == null || readinessChecker.getAsBoolean())
                                                    ? OK : HttpResponseStatus.SERVICE_UNAVAILABLE;
                                } else if (LIVE_URI.equals(uri)) {
                                    status = livenessChecker == null || !ready.get() || livenessChecker.getAsBoolean()
                                            ? OK : INTERNAL_SERVER_ERROR;
                                }
                                if (status != OK) {
                                    logger.warn("HTTP health check server returning " + status.code() + " for " + uri);
                                } else if (logger.isDebugEnabled()) {
                                    logger.debug("HTTP health check server returning " + status.code() + " for " + uri);
                                }
                                HttpResponse resp = new DefaultFullHttpResponse(HTTP_1_1, status);
                                resp.headers().setInt(CONTENT_LENGTH, 0);
                                if (keepAlive) {
                                    resp.headers().set(CONNECTION, KEEP_ALIVE);
                                }
                                ChannelFuture cf = ctx.writeAndFlush(resp);
                                if (!keepAlive) {
                                    cf.addListener(ChannelFutureListener.CLOSE);
                                }
                            }

                            @Override
                            public void channelReadComplete(ChannelHandlerContext ctx) {
                                ctx.flush();
                            }

                            @Override
                            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                ctx.close();
                                logger.error("Health probe http server channel failed", cause);
                            }
                        });
                    }
                });

        return b.bind(port).channel();
    }

    @Override
    public void close() {
        logger.info("Stopping litelinks health probe http server on port " + port);
        serverChannel.close();
    }

}
