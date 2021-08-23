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
package com.ibm.watson.litelinks;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.handler.ssl.SslContext;

import javax.net.ssl.SSLEngine;
import java.net.InetSocketAddress;
import java.util.List;

import static io.netty.buffer.Unpooled.directBuffer;

/**
 *
 */
public class FramedNettyTTransport extends NettyTTransport {

    public FramedNettyTTransport(Channel chan, ReaderTask reader, SslContext sslContext) {
        super(chan, reader, sslContext);
    }

    public FramedNettyTTransport(InetSocketAddress addr, long connectTimeout,
            ByteBufAllocator alloc, SslContext sslContext) {
        super(addr, connectTimeout, alloc, sslContext);
    }

    public FramedNettyTTransport(InetSocketAddress addr, long connectTimeout) {
        super(addr, connectTimeout);
    }

    // --------- inbound -------------

    //TODO *maybe* refactor this as a chan pipeline element

    @Override
    protected ChannelInboundHandler getReadHandler() {
        return new ReadHandler() {
            int fsize = -1;
            private final ByteBuf i32bufIn = directBuffer(4);

            @Override
            public void channelRead(ChannelHandlerContext ctx, Object msg) {
                ByteBuf data = (ByteBuf) msg;
                try {
                    int r = data.readableBytes();
                    if (r == 0) {
                        return;
                    }
                    while (true) {
                        if (fsize < 0) {
                            int wb = i32bufIn.writableBytes();
                            data.readBytes(i32bufIn);
                            if (r >= wb) {
                                // new frame
                                fsize = i32bufIn.readInt();
//								System.out.println("newframe: framesize = "+fsize);
                                if (r == wb) {
                                    return;
                                }
                                r -= wb;
                            } else {
                                return;
                            }
                        }
                        if (r <= fsize) {
                            enqueueReadBuf(data);
                            data = null; // don't release
                            if (r == fsize) {
                                fsize = -1;
                                i32bufIn.clear();
                            } else {
                                fsize -= r;
                            }
                            return;
                        }
                        enqueueReadBuf(data.readRetainedSlice(fsize));
                        r -= fsize;
                        fsize = -1;
                        i32bufIn.clear();
                        // continue to next frame
                    }
                } finally {
                    if (data != null) {
                        data.release();
                    }
                }
            }
        };
    }

    // --------- outbound -------------

    @Override
    protected void initializeChannel(Channel chan, SSLEngine sslEngine) {
        super.initializeChannel(chan, sslEngine);
        writeBuf.writeInt(0);
    }

    @Override
    public void flush() throws WTTransportException {
        final List<ByteBuf> pendingBufs = pendingWrites;
        ByteBuf wbuf = writeBuf();
        ByteBuf firstBuf = pendingBufs.isEmpty() ? wbuf : pendingBufs.get(0);
        ByteBuf unwrapped; // must unwrap in-case read-only
        while ((unwrapped = firstBuf.unwrap()) != null && unwrapped != firstBuf) {
            firstBuf = unwrapped;
        }
        firstBuf.setInt(0, pendingBytes + wbuf.writerIndex() - 4);
        super.flush();
        writeBuf().writeInt(0);
    }
}
