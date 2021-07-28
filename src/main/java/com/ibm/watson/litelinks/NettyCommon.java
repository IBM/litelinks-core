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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.ibm.watson.kvutils.OrderedShutdownHooks;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.FastThreadLocalThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadFactory;

public class NettyCommon {

    private static final Logger logger = LoggerFactory.getLogger(NettyCommon.class);

    private static final boolean useEpoll;
    private static final Class<? extends Channel> channelClass;

    static {
        useEpoll = Epoll.isAvailable() &&
                   !"false".equals(System.getProperty(LitelinksSystemPropNames.USE_EPOLL_TRANSPORT));
        logger.info(useEpoll? "Litelinks using native transport (epoll)"
                : "Litelinks using NIO transport");

        channelClass = useEpoll? EpollSocketChannel.class : NioSocketChannel.class;
    }

    public static boolean useEpoll() {
        return useEpoll;
    }

    public static Class<? extends Channel> getChannelClass() {
        return channelClass;
    }

    // Lazily initialized; uses daemon threads
    private static volatile EventLoopGroup workerGroup;

    public static EventLoopGroup getWorkerGroup() {
        EventLoopGroup wg = workerGroup;
        if (wg == null) {
            synchronized (NettyCommon.class) {
                if ((wg = workerGroup) == null) {
                    String propCount = System.getProperty(LitelinksSystemPropNames.WORKER_ELG_SIZE);
                    int count = propCount != null? Integer.parseInt(propCount)
                            : Math.min(8, Runtime.getRuntime().availableProcessors()); // default
                    logger.info("Creating litelinks shared worker ELG with " + count + " threads");
                    workerGroup = wg = newEventLoopGroup(count, "ll-elg-thread-%d");
                }
            }
        }
        return wg;
    }


    public static EventLoopGroup newEventLoopGroup(int size, String nameFormat) {
        ThreadFactory tfac = new ThreadFactoryBuilder().setDaemon(true)
                .setThreadFactory(FastThreadLocalThread::new)
                .setNameFormat(nameFormat).build();
        EventLoopGroup elg = useEpoll? new EpollEventLoopGroup(size, tfac)
                : new NioEventLoopGroup(size, tfac);
        OrderedShutdownHooks.addHook(3, elg::shutdownGracefully);
        return elg;
    }
}
