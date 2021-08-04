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

import com.ibm.watson.litelinks.FramedNettyTTransport;
import com.ibm.watson.litelinks.LitelinksSystemPropNames;
import com.ibm.watson.litelinks.LitelinksTProtoExtension;
import com.ibm.watson.litelinks.NettyCommon;
import com.ibm.watson.litelinks.NettyTTransport;
import com.ibm.watson.litelinks.NettyTTransport.ReaderTask;
import com.ibm.watson.litelinks.TTimeoutException;
import com.ibm.watson.litelinks.ThreadPoolHelper;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.AbstractNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.TimeUnit.*;

/**
 * Netty-based Thrift server impl. Takes similar configuration parameters
 * to Thrift's {@link org.apache.thrift.server.TThreadedSelectorServer}.
 * Is nonblocking but does not extend {@link AbstractNonblockingServer},
 * and does not make use of a separate {@link TServerTransport}.
 */
public class NettyTServer extends TServer implements ListeningService {

    private static final Logger logger = LoggerFactory.getLogger(NettyTServer.class);

    private static final Class<? extends ServerChannel> serverChanClass
            = NettyCommon.useEpoll() ? EpollServerSocketChannel.class
            : NioServerSocketChannel.class;

    static final AttributeKey<NettyTTransport> NTT_ATTR_KEY =
            AttributeKey.newInstance("NTT_CHAN_ATTR");


//	private static final boolean deferReading = true;

    /**
     * @see LitelinksSystemPropNames#SERVER_READ_TIMEOUT
     */
    private final long readTimeoutMillis = 1000L *
            Long.getLong(LitelinksSystemPropNames.SERVER_READ_TIMEOUT, 20L);

    /**
     * @see LitelinksSystemPropNames#CANCEL_ON_CLIENT_CLOSE
     */
    private final boolean cancelOnClientClose
            = Boolean.getBoolean(LitelinksSystemPropNames.CANCEL_ON_CLIENT_CLOSE);

    protected final ServerBootstrap sbs;
    protected Channel serverChannel;

    public final boolean framed;

    protected final InetSocketAddress specifiedAddress;
    protected InetSocketAddress actualAddress;

    protected final SslContext sslContext;

    protected long shutdownTimeoutNanos; // amount of time to give requests to finish
    protected volatile boolean shutdown;
    protected final ChannelGroup cgroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

    protected final ExecutorService appThreads;
    protected final boolean userProvidedExecutor;

    public NettyTServer(TThreadedSelectorServer.Args args) {
        super(args);
        this.specifiedAddress = args.bindAddr;
        TimeUnit stu = args.getStopTimeoutUnit();
        if (stu != null) {
            shutdownTimeoutNanos = NANOSECONDS.convert(args.getStopTimeoutVal(), stu);
        }
        if (shutdownTimeoutNanos <= 0) {
            shutdownTimeoutNanos = NANOSECONDS.convert(30, SECONDS);
        }
        this.framed = inputTransportFactory_ instanceof TFramedTransport.Factory;

        ExecutorService userThreads = args.getExecutorService();
        userProvidedExecutor = userThreads != null;
        appThreads = userProvidedExecutor? userThreads : createAppThreads(args.getWorkerThreads());
        sslContext = args.sslContext;
        sbs = new ServerBootstrap()
                .group(getBossGroup(), NettyCommon.getWorkerGroup())
                .channel(serverChanClass)
                .option(ChannelOption.SO_BACKLOG, 128) //TODO TBD
                .childOption(ChannelOption.SO_KEEPALIVE, true)
//		        .childOption(ChannelOption.AUTO_READ, !deferReading)
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(final SocketChannel ch) throws Exception {
                        if (shutdown || !isServing()) {
                            ch.close();
                            return;
                        }
                        cgroup.add(ch);
                        logger.debug("New server proc task");
                        boolean ok = false;
                        try {
                            // This creates a new NettyTTransport
                            // and connects it to the channel.
                            ProcessTask procTask = new ProcessTask(ch);
                            // The task is also scheduled for the first time
                            // here since it's assumed data (new req) will
                            // immediately follow opening of the channel
                            // (but not in TLS case where there will be a
                            // delay for the handshake).
                            if (sslContext == null) {
                                procTask.schedule();
                            }
                            ok = true;
                        } finally {
                            if (!ok) ch.close();
                        }
                    }
                });
    }

    /**
     * This task gets scheduled each time a new request comes in on the channel.
     * Channel/TTransport/ProcessTask are 1-1-1
     */
    protected class ProcessTask implements Runnable, ReaderTask {
        private final TProtocol intp, outtp;
        private final TProcessor tproc;
        private Future<?> future;

        private long lastReqStartNanos;
        private long requestCounter;

        private ProcessTask(Channel ch) {
            NettyTTransport ntt = framed? new FramedNettyTTransport(ch, this, sslContext)
                    : new NettyTTransport(ch, this, sslContext);
            ch.attr(NTT_ATTR_KEY).set(ntt);
            intp = inputProtocolFactory_.getProtocol(ntt);
            outtp = outputProtocolFactory_.getProtocol(ntt);
            tproc = processorFactory_.getProcessor(ntt);
            if (tproc instanceof LitelinksTProtoExtension.Processor) {
                ((LitelinksTProtoExtension.Processor) tproc).setClearThreadLocals(false);
            }
        }

        // called from within NTT readBufsReady lock
        @Override
        public boolean schedule() {
            lastReqStartNanos = System.nanoTime();
            try {
                future = appThreads.submit(this);
                return true;
            } catch (RejectedExecutionException ree) {
                logger.warn("Rejecting new request received after shutdown");
                return false;
            }
        }

        // called from within NTT readBufsReady lock
        @Override
        public void abort() {
            if (!cancelOnClientClose) {
                return;
            }
            Future<?> fut = future;
            if (fut != null) {
                boolean cancelled = fut.cancel(true); // interrupt if already running
                if (cancelled) {
                    logger.info("Aborted in-flight service req after client disconnect."
                            + " Lasted " + msSince(lastReqStartNanos)
                            + "ms, with " + requestCounter + " prior reqs on channel");
                }
            }
        }

        @Override
        public void newDataReady() {
            lastReqStartNanos = System.nanoTime();
        }

        @SuppressWarnings("resource")
        @Override
        public void run() {
            NettyTTransport ntt = null;
            ServerRequestThread ourThread = null;
            try {
                Thread currentThread = Thread.currentThread();
                if (currentThread instanceof ServerRequestThread) {
                    ourThread = (ServerRequestThread) currentThread;
                }
                ntt = (NettyTTransport) intp.getTransport();
                do {
                    if (ourThread != null) {
                        ourThread.startRequest(lastReqStartNanos);
                    }
                    try {
                        // this timer is cancelled in LitelinksTProto.InterceptTInProto#readMessageEnd()
                        ntt.startIOTimer(readTimeoutMillis);
                        tproc.process(intp, outtp);
                    } catch (TTimeoutException tte) {
                        if (!tte.isBeforeReading()) {
                            throw new TTimeoutException("Timed out reading request from client", tte);
                        }
                        // else while clause below should return false, no need to close the channel
                    }
                    // *always* cleanup the ThreadContext/MDC (even it they weren't sent in
                    // the latest request, they could have been set by the service impl)
                    LitelinksTProtoExtension.Processor.clearThreadLocals();
                    requestCounter++;
                }
                while (ntt.newDataIsReady());
//				if(!shutdown) ntt = null;
                ntt = null;
            } catch (TException te) {
                // checked exceptions thrown by the service impl go back over the wire
                // and are logged in the thrift code. checked exceptions which reach
                // here must be protocol/transport related (e.g. connections closed mid-process)
                String methodName = ourThread != null? ourThread.getMethodName() : null;
                String msg = "A client connection terminated before processing"
                        + (methodName != null? " of method " + methodName : "")
                        + " had completed, after " + msSince(lastReqStartNanos)
                        + "ms and " + requestCounter + " prior reqs processed: ";
                if (!logger.isDebugEnabled()) {
                    logger.warn(msg + te);
                } else {
                    logger.warn(msg, te); //also log stacktrace
                }
            } catch (Throwable t) {
                logger.error("Request processing failed with unchecked exception"
                        + " after channel processed " + requestCounter + " reqs", t);
                if (t instanceof Error) {
                    throw t;
                }
            } finally {
                if (ourThread != null) {
                    ourThread.reset();
                }
                // ntt non-null => exception
                if (ntt != null) {
                    // TLs cleared here in exception case so MDC included in catch-block error logs
                    LitelinksTProtoExtension.Processor.clearThreadLocals();
                    if (ntt.isOpen()) {
                        ntt.close(); // close this channel
                    }
                }
            }
        }
    }

    static long msSince(long nanoTime) {
        return MILLISECONDS.convert(System.nanoTime() - nanoTime, NANOSECONDS);
    }

    private class NettyTServerThread extends ServerRequestThread {
        public NettyTServerThread(Runnable r, long num) {
            super(r, "ll-server-thread-" + num);
        }

        @Override
        protected boolean logInterrupt(long runningTimeMillis) {
            return super.logInterrupt(runningTimeMillis) && !appThreads.isShutdown();
        }
    }

    private ExecutorService createAppThreads(int workThreads) {
        final ThreadFactory threadFactory = new ThreadFactory() {
            final AtomicLong count = new AtomicLong();

            @Override
            public Thread newThread(Runnable r) {
                // note these are intentionally *not* daemons
                return new NettyTServerThread(r, count.incrementAndGet());
            }
        };
        int coreThreads = Math.min(Runtime.getRuntime().availableProcessors(), 4);
        if (workThreads <= coreThreads) { // includes workThreads <= 0 (means unlimited)
            int maxThreads;
            BlockingQueue<Runnable> queue;
            if (workThreads > 0) {
                // fixed pool size
                if (workThreads < coreThreads) {
                    coreThreads = workThreads;
                }
                maxThreads = workThreads;
                queue = new LinkedBlockingQueue<Runnable>();
            } else {
                // unbounded pool
                maxThreads = Integer.MAX_VALUE;
                queue = new SynchronousQueue<Runnable>();
            }
            return new ThreadPoolExecutor(coreThreads, maxThreads, 30L, MINUTES, queue, threadFactory);
        }
        // workThreads > coreThreads ==> capped pool
        return ThreadPoolHelper.newThreadPool(coreThreads, workThreads, 30L, MINUTES, threadFactory);
    }

    @Override
    public SocketAddress getListeningAddress() {
        return actualAddress != null? actualAddress : specifiedAddress;
    }

    public void setShutdownTimeout(int val, TimeUnit unit) {
        shutdownTimeoutNanos = NANOSECONDS.convert(val, unit);
    }

    @Override
    public void serve() {
        final Channel chan;
        //TODO maybe check for reuse here & throw IllegalStateException

        // Bind and start to accept incoming connections.
        chan = sbs.bind(specifiedAddress).syncUninterruptibly().channel(); //TODO timeout here?
        chan.closeFuture().addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) {
                setServing(false);
            }
        });
        actualAddress = (InetSocketAddress) chan.localAddress();
        serverChannel = chan;

        logger.info("service listening on " + actualAddress);

        TServerEventHandler eh = eventHandler_;
        try {
            if (eh != null) {
                eh.preServe();
            }
            // don't start processing incoming connections if preServe() fails
            setServing(true);
        } catch (RuntimeException re) {
            logger.warn("TServerEventHandler threw unchecked exception", re);
        }

        synchronized (this) {
            while (!shutdown) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    // shutdown (unexpected interruption)
                    shutdown = true;
                    logger.warn("NettyTServer main thread interrupted, shutting down");
                    Thread.currentThread().interrupt();
                }
            }
        }

        gracefulShutdown(chan, System.nanoTime() + shutdownTimeoutNanos);
    }

    @Override
    public void stop() {
        if (shutdown) {
            return; // we are already stopped or not yet started
        }
        synchronized (this) {
            if (shutdown) {
                return;
            }
            shutdown = true;
            notify();
        }
    }

    protected void gracefulShutdown(Channel chan, long deadline) {
        logger.info("Beginning graceful shutdown...");
        // graceful shutdown sequence:
        // 1. stop accepting new connections
        ChannelFuture cf = chan.close();
        // 2. 100 ms grace period for data from in-flight new reqs
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        if (!userProvidedExecutor) {
            // 3. initiate app threadpool shutdown
            appThreads.shutdown();
            logger.info("Triggering shutdown of idle app threads...");

            // 4. wait for in-progress invocations to complete
            boolean done = false;
            try {
                long remain = deadline - System.nanoTime();
                done = appThreads.awaitTermination(Math.max(remain,
                        NANOSECONDS.convert(40, MILLISECONDS)), NANOSECONDS);
                if (done) {
                    logger.info("App threads completed cleanly");
                } else {
                    logger.info(cgroup.size() + " invocations still "
                            + "in progress after shutdown timeout, force closing");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        } else {
            try {
                for (Channel c : cgroup) {
                    NettyTTransport ntt = c.attr(NTT_ATTR_KEY).get();
                    if (ntt != null) {
                        long remain = deadline - System.nanoTime();
                        ntt.waitUntilUnscheduled(Math.max(remain,
                                NANOSECONDS.convert(20, MILLISECONDS)));
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        // 5. wait for channels to close
        long remain = deadline - System.nanoTime(), wait = Math.max(remain,
                NANOSECONDS.convert(300, MILLISECONDS));
        cgroup.close().awaitUninterruptibly(wait, NANOSECONDS);
        int r = cgroup.size();
        if (r > 0) {
            logger.warn(r + " channels didn't close in "
                    + MILLISECONDS.convert(wait, NANOSECONDS) + "ms");
        }

        // 6. wait for server channel close to finish
        remain = deadline - System.nanoTime();
        if (remain > 0) {
            cf.awaitUninterruptibly(remain, NANOSECONDS);
        }
    }

    // --------------- shared boss EventLoop -------------

    private static volatile EventLoopGroup bossGroup;

    protected static EventLoopGroup getBossGroup() {
        EventLoopGroup bg = bossGroup;
        if (bg == null) {
            synchronized (NettyTServer.class) {
                if ((bg = bossGroup) == null) {
                    bossGroup = bg = NettyCommon.newEventLoopGroup(1, "ll-boss-elg-thread");
                }
            }
        }
        return bg;
    }

    protected static Class<? extends ServerChannel> getServerChannelClass() {
        return serverChanClass;
    }

}
