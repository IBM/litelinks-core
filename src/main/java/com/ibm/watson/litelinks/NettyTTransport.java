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

import com.google.common.io.BaseEncoding;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufHelper;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.Future;
import org.apache.thrift.TConfiguration;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSession;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.netty.buffer.Unpooled.unreleasableBuffer;

/**
 * Netty-based TTransport
 */
public class NettyTTransport extends TTransport {

    private static final Logger logger = LoggerFactory.getLogger(NettyTTransport.class);
    private static final boolean trace = logger.isTraceEnabled();

    static final long M = 1_000_000L;

    private final SocketAddress addr;
    private final Bootstrap bs;

    protected final long connectTimeout; // in millisecs

    private long deadline; // set per rpc invocation, in nanosecs

    private Channel channel; // set upon successful open, null when closed

    private final ReaderTask reader; // server side only (null for client-side)
    private boolean readerScheduled;

    protected boolean used;
    protected Throwable failure;

    // true as soon as *any* packets are received, including for SSL handshake
    protected boolean dataReceived;

    /*
     * TODO:
     * - more logging
     * - optimizations:
     *      - make sure pooled buffers get used (maybe use custom allocator)
     * - max data size safeguards / max new chan rate limit
     *
     * [- refac framed read handler maybe.. to be pipeline addition]
     *
     *
     */

    // used by clients
    public NettyTTransport(InetSocketAddress addr, long connectTimeout,
            ByteBufAllocator alloc, final SslContext sslContext) {
        if (addr == null) {
            throw new IllegalArgumentException();
        }
        this.addr = addr;
        this.connectTimeout = connectTimeout;
		this.bs = new Bootstrap()
                .group(NettyCommon.getWorkerGroup()).channel(NettyCommon.getChannelClass())
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addFirst(new ChannelInboundHandlerAdapter() {
                            @Override
                            public void channelRead(ChannelHandlerContext ctx, Object msg) {
                                dataReceived = true;
                                ctx.fireChannelRead(msg);
                                // remove self from pipeline after first read
                                ctx.pipeline().remove(this);
                            }
                        });
                        SSLEngine sslEngine = null;
                        if (sslContext != null) {
                            sslEngine = sslContext.newEngine(alloc,
                                    addr.getHostString(), addr.getPort());
                        }
                        initializeChannel(ch, sslEngine);
                    }

                })
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.ALLOCATOR, alloc);
		this.reader = null;
		this.readerScheduled = true; // prevents any schedule attempts
    }

    public NettyTTransport(InetSocketAddress addr, long connectTimeout) {
        this(addr, connectTimeout, PooledByteBufAllocator.DEFAULT, null);
    }

    /**
     * Used by server-side. It is assumed the reader will be scheduled externally
     * if and only if sslContext is null.
     */
    public NettyTTransport(Channel chan, ReaderTask reader, SslContext sslContext) {
        if (chan == null) {
            throw new IllegalArgumentException();
        }
        this.addr = chan.remoteAddress();
        this.connectTimeout = 0; // already open in this case
        this.bs = null;
        this.reader = reader;
		this.readerScheduled = sslContext == null;

        SSLEngine sslEngine = sslContext == null? null
                : sslContext.newEngine(chan.alloc());

        initializeChannel(chan, sslEngine);
    }

    @Override
    public TConfiguration getConfiguration() {
        return TConfiguration.DEFAULT;
    }

    /**
     * Called by client prior to invoking a service API
     *
     * @param timeoutMillis milliseconds, or 0 for none
     */
    public void startIOTimer(long timeoutMillis) {
        if (timeoutMillis == 0L) {
            deadline = 0L;
        } else {
            final long d = System.nanoTime() + timeoutMillis * M;
            deadline = d == 0L? 1L : d;
        }
        dataWritten = false;
    }


    @Override
    public synchronized void open() throws WTTransportException {
        if (isOpen()) {
            throw new WTTransportException(TTransportException.ALREADY_OPEN, true);
        }
        // for now not supporting re-open
        if (used) {
            throw new WTTransportException("Can't reopen used NettyTTransport", true);
        }

        long before = System.nanoTime();
        final ChannelFuture cf = bs.connect(addr);
        syncChanFuture(cf, connectTimeout, "opening new channel failed: " + addr, false);

        long remaining = connectTimeout - (System.nanoTime() - before) / M;
        waitForSslHandshake(cf.channel(), remaining);
    }

    protected void initializeChannel(final Channel chan, SSLEngine sslEngine) {
        chan.closeFuture().addListener(channelClosedListener());
        ChannelPipeline pipeline = chan.pipeline();
        if (sslEngine != null) {
            pipeline.addLast(new SslHandler(sslEngine));
        }
        ByteBuf wbuf = chan.alloc().ioBuffer(512);
        writeBuf = wbuf.capacity(wbuf.maxFastWritableBytes());
        arrayReadBufs = wbuf.hasArray();
        pipeline.addLast(getReadHandler());
        channel = chan;
    }

    /**
     * @param cf
     * @param timeoutMillis Long.MAX_VALUE for indefinite
     * @param failMsg
     * @param
     * @throws WTTransportException for timeout, interrupt, or IO exception
     */
    protected void syncChanFuture(ChannelFuture cf, long timeoutMillis, String failMsg,
            boolean forceAbortOnTimeoutOrInterrupt) throws WTTransportException {
        try {
            if (timeoutMillis == Long.MAX_VALUE) {
                cf.await();
            } else if (!cf.await(timeoutMillis)) {
                abort(cf, forceAbortOnTimeoutOrInterrupt);
                throw new TTimeoutException(failMsg, !dataWritten);
            }
        } catch (InterruptedException e) {
            abort(cf, forceAbortOnTimeoutOrInterrupt);
            throw new WTTransportException(failMsg, e, !dataWritten);
        }
        if (!cf.isSuccess()) {
            cf.channel().close();
            throw new WTTransportException(failMsg, cf.cause(), !dataWritten);
        }
    }

    protected void abort(ChannelFuture cf, boolean immediate) {
        if (immediate) {
            cf.cancel(true);
            cf.channel().close();
        } else {
            cf.addListener(CLOSE_FUTURE_LISTENER);
        }
    }

    private static final ChannelFutureListener CLOSE_FUTURE_LISTENER = future -> {
        Channel chan = future.channel();
        if (chan != null) {
            chan.close();
        }
    };

    private void waitForSslHandshake(Channel chan, long timeoutMillis) throws WTTransportException {
        SslHandler sh = chan.pipeline().get(SslHandler.class);
        if (sh != null) {
            try {
                Future<Channel> hf = sh.handshakeFuture();
                try {
                    if (!hf.await(timeoutMillis)) {
                        String message = dataReceived?
                                "Timed out during SSL handshake"
                                : "No response received from remote address: " + addr;
                        throw new TTimeoutException(message, true);
                    }
                } catch (InterruptedException e) {
                    throw new WTTransportException(e, true);
                }
                Throwable fail = hf.cause();
                if (fail != null) {
                    throw new WTTransportException("SSL handshake failed: " + fail, fail, true);
                }
            } catch (WTTransportException tte) {
                chan.close();
                throw tte;
            }
        }
    }

    public SocketAddress getRemoteAddress() {
        final Channel c = channel;
        return c != null? c.remoteAddress() : null;
    }

    public SSLSession getSslSession() {
        final Channel c = channel;
        if (c == null) {
            return null;
        }
        SslHandler sh = c.pipeline().get(SslHandler.class);
        return sh != null? sh.engine().getSession() : null;
    }

    @Override
    public boolean isOpen() {
        final Channel c = channel;
        return c != null && c.isOpen();
    }

    /**
     * Asynchronous
     */
    @Override
    public synchronized void close() {
        final Channel chan = channel;
        if (chan != null) {
            chan.close(); // .awaitUninterruptibly();
        }
    }

    private ChannelFutureListener channelClosedListener() {
        return new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) {
                future.channel().read(); // flush any remaining data
                if (failure == null) {
                    failure = future.cause();
                }
                used = true;
                channel = null;
                // wake up client thread if waiting for data
                synchronized (readBufsReady) {
                    if (reader != null && readerScheduled) {
                        reader.abort();
                    }
                    readBufsReady.notify();
                }
                // give client thread time to finish with buffers
                future.channel().eventLoop().schedule(() -> {
                    logger.debug("releasing buffers");
                    synchronized (readBufsReady) {
                        readBufsReady.set(0);
                        readBufsReady.notify();
                    }
                    releaseAll(rbbq);
                    rbb = null;
                    if (overflow != null) {
                        ByteBuf[] bbq;
                        while ((bbq = overflow.poll()) != null) {
                            releaseAll(bbq);
                        }
                    }
                    if (writeBuf != null) {
                        writeBuf.release();
                        writeBuf = null;
                    }
                    for (ByteBuf buf : pendingWrites) {
                        if (buf != null) buf.release();
                    }
                    pendingWrites.clear();
                }, 1L, TimeUnit.SECONDS);
            }
        };
    }

    // only called from client thread
    protected final Channel chan() throws TConnectionClosedException {
        final Channel c = channel;
        if (c == null) {
            chanClosed();
        }
        return c;
    }

    // only called from client thread
    private void chanClosed() throws TConnectionClosedException {
//		System.out.println("dbg: about to throw, used="+used+" ntt="+this+", datawritten="+dataWritten);
        throw new TConnectionClosedException(failure, !dataWritten);
    }

    @Override
    protected void finalize() throws Throwable {
        if (isOpen()) {
            close();
        }
    }

    // --------- inbound ----------

    /**
     * Used for dispatching processing task on server side
     */
    public interface ReaderTask {
        /**
         * called to schedule reader task when new data arrives
         *
         * @return false if could not be scheduled
         */
        boolean schedule();
        /**
         * called to abort already-scheduled reader task if conn disconnects
         */
        void abort();
        /**
         * called-back from from within {@link NettyTTransport#newDataIsReady()}
         * when there is new data (indicating next request on the server channel)
         */
        void newDataReady();
    }

    private static final int INIT_BUF_Q_SIZE = 32;

    // circular queues of buffers
    private ByteBuf[] rbbq = new ByteBuf[INIT_BUF_Q_SIZE], wbbq = rbbq;
    private int readIdx, writeIdx;
    private ArrayDeque<ByteBuf[]> overflow; // used only if writer catches up with reader
    private ByteBuf rbb; // == rbbq[readIdx]
    // true once any data has been consumed from this transport/channel, false prior to that
    private boolean dataRead;

    protected final AtomicInteger readBufsReady = new AtomicInteger();

    protected class ReadHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (trace) {
                logger.trace("channelRead: " + ByteBufUtil.hexDump((ByteBuf) msg));
            }
            enqueueReadBuf((ByteBuf) msg);
        }
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            if (failure == null) {
                failure = cause;
            } else {
                logger.warn("DEBUG: Second eC thrown for " + NettyTTransport.this
                            + " (first was: " + failure + ")", cause);
            }
//			ctx.close(); // TBD
        }
    }

    private static void releaseAll(ByteBuf[] queue) {
        if (queue != null) {
            for (int i = 0; i < queue.length; i++) {
                ByteBuf bb = queue[i];
                if (bb != null) {
                    bb.release();
                    queue[i] = null;
                }
            }
        }
    }

    protected ChannelInboundHandler getReadHandler() {
        return new ReadHandler();
    }

    /**
     * enqueued ByteBufs are released by consuming (client) thread
     */
    protected void enqueueReadBuf(ByteBuf rb) {
        int wqlen = wbbq.length, rbr = readBufsReady.get();
        // check if we need to overflow into a new buffer array
        if (rbr >= wqlen &&
            (wbbq == rbbq || writeIdx == 0 && wbbq[0] != null)) {
            wbbq = new ByteBuf[wqlen = wbbq.length + wbbq.length / 2];
            if (overflow == null) {
                overflow = new ArrayDeque<>(4);
            }
            synchronized (overflow) {
                overflow.add(wbbq);
            }
            writeIdx = 0;
        }
        wbbq[writeIdx] = rb;
        boolean abort = false;
        synchronized (readBufsReady) {
            readBufsReady.incrementAndGet();
            readBufsReady.notify();
            if (!readerScheduled && channel != null) {
                abort = !reader.schedule();
                readerScheduled = true;
            }
        }
        writeIdx = writeIdx == wqlen - 1? 0 : writeIdx + 1;
        if (abort) {
            close(); // bail if process task can't be scheduled
        }
    }

    @Override
    public int read(byte[] buf, int off, int len) throws WTTransportException {
        if (len == 0) {
            return 0; //tbd
        }
        waitForReadBuf();
        int read = 0;
        while (read < len) {
            if (rbb == null) {
                moveToNextOverflow();
            }
            int rb = rbb.readableBytes(), toread = Math.min(rb, len - read);
            rbb.readBytes(buf, off + read, toread);
			if(trace) logger.trace("read: " + hexString(buf, off + read, toread));
            read += toread;
            if (toread >= rb && readBufferConsumed(false)) {
                break;
            }
        }
        return read;
    }

    /**
     * read len bytes. the returned ByteBuf must be subsequently released
     */
    public ByteBuf readAll(int len) throws WTTransportException {
        if (len <= 0) {
            return Unpooled.EMPTY_BUFFER;
        }
        ByteBuf bb = null;
        CompositeByteBuf cbb = null;
        try {
            boolean waitNeeded = true;
            while (true) {
                if (waitNeeded) {
                    waitForReadBuf();
                }
                if (rbb == null) {
                    moveToNextOverflow();
                }
                int rb = rbb.readableBytes(), toread = Math.min(rb, len);
                if (toread == rb) {
                    bb = rbb;
                    waitNeeded = readBufferConsumed(true);
                } else {
                    bb = rbb.readRetainedSlice(toread);
                    waitNeeded = false;
                }
	            if(trace) logger.trace("read: " + ByteBufUtil.hexDump(bb));
                if ((len -= toread) <= 0) {
                    ByteBuf result = cbb != null? cbb.addComponent(true, bb) : bb;
                    bb = null;
                    cbb = null;
                    return result;
                }
                if (cbb == null) {
                    cbb = chan().alloc().compositeBuffer(Integer.MAX_VALUE);
                }
                cbb.addComponent(true, bb);
                bb = null;
            }
        } finally {
            // ensure we don't leak the cumulation in error cases
	        if (bb != null) bb.release();
	        if (cbb != null) cbb.release();
        }
    }

    /**
     * read len bytes, if they fall within a single underlying buffer. the returned
     * ByteBuf <b>must</b> be subsequently released
     */
    public ByteBuf readIfSingleBuf(int len) throws WTTransportException {
        if (len <= 0) {
            return Unpooled.EMPTY_BUFFER;
        }
        waitForReadBuf();
        if (rbb == null) {
            moveToNextOverflow();
        }
        if (rbb.nioBufferCount() != 1) {
            return null;
        }
        int rb = rbb.readableBytes();
        if (rb < len) {
            return null;
        }
        if (rb > len) {
            return rbb.readRetainedSlice(len);
        }
        readBufferConsumed(true); // rb == len
        return rbb;
    }

    /**
     * read len bytes. The returned ByteBuf <b>must</b> be subsequently released
     */
    public ByteBuf readAsSingleBuf(int len) throws WTTransportException {
        if (len <= 0) {
            return Unpooled.EMPTY_BUFFER;
        }
        waitForReadBuf();
        if (rbb == null) {
            moveToNextOverflow();
        }
        int rb = rbb.readableBytes();
        if (rbb.nioBufferCount() == 1 && rb >= len) {
            if (rb > len) {
                return rbb.readRetainedSlice(len);
            }
            ByteBuf readBuf = rbb;
            readBufferConsumed(true);
            return readBuf.touch();
        }
        ByteBuf newBuf = chan().alloc().ioBuffer(len);
        try {
            while (true) {
                int toread = Math.min(rb, len);
                newBuf.writeBytes(rbb, toread);
                boolean waitNeeded = toread == rb && readBufferConsumed(false);
                if ((len -= toread) <= 0) {
                    return newBuf;
                }
                if (waitNeeded) {
                    waitForReadBuf();
                }
                if (rbb == null) {
                    moveToNextOverflow();
                }
                rb = rbb.readableBytes();
            }
        } finally {
            // ensure we don't leak buf in error cases
            if (len > 0) newBuf.release();
        }
    }

    public byte read() throws WTTransportException {
        waitForReadBuf();
        if (rbb == null) {
            moveToNextOverflow();
        }
        byte b = rbb.readByte();
        if (!rbb.isReadable()) {
            readBufferConsumed(false);
        }
        return b;
    }

    private void waitForReadBuf() throws WTTransportException {
        remain(); // throws timeout if expired
        int bufsReady;
        if (rbb == null) {
            bufsReady = readBufsReady.get();
            if (bufsReady < 1 && (bufsReady = waitForNewData()) <= 0) {
                chanClosed(); // will throw TConnectionClosedException
            }
            rbb = rbbq[readIdx];
        }
        dataRead = true;
    }

    // returns true if no more buffers are ready
    private boolean readBufferConsumed(boolean ownershipTransferred) {
        final ByteBuf bb = rbb;
        rbbq[readIdx] = rbb = null;
        if (!ownershipTransferred) {
            bb.release();
        }
        readIdx = readIdx == rbbq.length - 1? 0 : readIdx + 1;
        if (readBufsReady.decrementAndGet() <= 0) {
            return true;
        }
        rbb = rbbq[readIdx];
        return false;
    }

    private void moveToNextOverflow() {
        synchronized (overflow) {
            rbb = (rbbq = overflow.poll())[readIdx = 0];
        }
    }

    // The four methods below only get used in the case of non-direct (heap-backed) netty bytebufs

    private boolean arrayReadBufs;

    @Override
    public int getBytesRemainingInBuffer() {
        if (!arrayReadBufs) {
            return -1;
        }
        if (readBufsReady.get() < 1) {
            return 0;
        }
        if (rbb == null && (rbb = rbbq[readIdx]) == null) {
            moveToNextOverflow();
        };
        return rbb.readableBytes();
    }

    @Override
    public void consumeBuffer(int len) {
        final ByteBuf bb = rbb;
        if (trace) logger.trace("consumed: " + hexString(bb.array(), getBufferPosition(), len));
        if (!bb.skipBytes(len).isReadable()) {
            readBufferConsumed(false);
        }
    }

    @Override
    public int getBufferPosition() {
        final ByteBuf bb = rbb;
        return bb.arrayOffset() + bb.readerIndex();
    }

    @Override
    public byte[] getBuffer() {
        final ByteBuf bb = rbb;
        return bb != null && bb.hasArray()? bb.array() : null;
    }

    @Override
    public boolean peek() {
        return readBufsReady.get() > 0;
    }

    @Override
    public void checkReadBytesAvailable(long arg0) throws TTransportException {
        // Introduced in thrift 0.14.0 and only used for verification
    }

    @Override
    public void updateKnownMessageSize(long paramLong) throws TTransportException {
        // Not used by this transport
    }

    /*
     * >0 buffers ready
     * 	0 continue waiting
     * -1 chan closed
     */
    private int bufferReadyCount() {
        int rbr = readBufsReady.get();
        return rbr > 0? rbr : channel == null? -1 : 0;
    }

    /**
     * Should only be called from client thread. Won't return until either
     * some data is available to read or the channel has been closed.
     *
     * @return number of new incoming buffers ready in queue, or -1 if chan is closed
     * @throws WTTransportException if interrupted
     * @throws TTimeoutException    if timeout expires while waiting
     */
    public int waitForNewData() throws WTTransportException {
        try {
            int rbr;
            synchronized (readBufsReady) {
                if (deadline == 0L) { // indefinite
                    while ((rbr = bufferReadyCount()) == 0) {
                        readBufsReady.wait();
                    }
                } else {
                    long rem;
                    while ((rbr = bufferReadyCount()) == 0 && (rem = remain()) > 0) {
                        readBufsReady.wait(rem);
                    }
                }
            }
            if (rbr < 0 && failure != null) {
                throw new WTTransportException(failure);
            }
            return rbr;
        } catch (InterruptedException e) {
            throw new WTTransportException(e);
        }
    }

    // used on server side only
    public boolean newDataIsReady() throws WTTransportException {
        boolean ready;
        synchronized (readBufsReady) {
            ready = readBufsReady.get() > 0;
            if (reader != null) {
                if (ready) {
                    reader.newDataReady();
                } else {
                    readerScheduled = false;
                }
            }
        }
        if (!ready && failure != null) {
            throw new WTTransportException(failure); //TODO TBD
        }
        return ready;
    }

    // used on server side only, during shutdown
    public void waitUntilUnscheduled(long nanosecs) throws InterruptedException {
        long deadlineNanos = System.nanoTime() + nanosecs;
        synchronized (readBufsReady) {
            while (readerScheduled) {
                long remainMillis = (deadlineNanos - System.nanoTime()) / M;
                if (remainMillis <= 0) {
                    return;
                }
                readBufsReady.wait(remainMillis);
            }
        }
    }

    /**
     * Returns time remaining before timeout in milliseconds, or throws
     * {@link TTimeoutException} if timeout has already expired.
     *
     * @return Long.MAX_VALUE if no timeout or remaining time in ms otherwise
     * @throws TTimeoutException if time has expired
     */
    protected final long remain() throws TTimeoutException {
        if (deadline == 0L) {
            return Long.MAX_VALUE;
        }
        // note must divide before >0 check or could miss timeout
        final long remain = (deadline - System.nanoTime()) / M;
        if (remain > 0L) {
            return remain;
        }
        Channel chan = channel;
        TTimeoutException timeout = new TTimeoutException(!dataWritten, !dataRead);
        if (failure != null) {
            failure = timeout;
        }
        if (chan != null) {
            chan.close();
        }
        throw timeout;
    }


    // ---------------------- outbound -------------------------

    protected static final int MAX_WRITEBUF_SIZE = 8 * 1024 * 1024; // 8MiB

    protected ByteBuf writeBuf, unreleasableWriteBuf;
    protected int dupsUsed;
    protected final List<ByteBuf> writeBufDupPool = new ArrayList<>(4);
    protected final List<ByteBuf> pendingWrites = new ArrayList<>(8);
    protected int pendingBytes; // doesn't include those in current writeBuf, used only in framed case
    protected boolean dataWritten = true; // reflected in thrown transport exceptions

    protected final ByteBuf writeBuf() throws TConnectionClosedException {
        ByteBuf wbuf = writeBuf;
        if (wbuf == null) {
            chanClosed();
        }
        return wbuf;
    }

    protected final ByteBuf writeBuf(int ensureSize) throws TConnectionClosedException {
        ByteBuf wbuf = writeBuf();
        int wi = wbuf.writerIndex(), cap = wbuf.capacity(), requiredCap = wi + ensureSize;
        if (cap >= requiredCap) {
            return wbuf;
        }
        if (requiredCap >= 4096) {
            return allocateNewWriteBuf(Math.min(cap << 1, MAX_WRITEBUF_SIZE));
        }
        wbuf.capacity(requiredCap);
        // This is done as a second step since maxFastWritableBytes may depend on the first capacity increase
        return wbuf.capacity(wi + wbuf.maxFastWritableBytes());
    }

    private ByteBuf allocateNewWriteBuf(int size) throws TConnectionClosedException {
        // Add current writebuf to pendingWrites list and allocate a new one
        ByteBuf before = writeBuf(), wbuf;
        writeBuf = wbuf = chan().alloc().ioBuffer(size);
        pendingWrites.add(before);
        pendingBytes += before.writerIndex();
        // Reset derived buffers
        unreleasableWriteBuf = null;
        writeBufDupPool.clear();
        dupsUsed = 0;
        return wbuf.capacity(wbuf.maxFastWritableBytes());
    }

    //TODO(maybe) send/flush after certain # bytes accumulated (non-framed)

    protected static final int WRITE_DIRECT_LEN = 96;

    @Override
    public void write(byte[] buf, int off, int len) throws WTTransportException {
        if (channel == null) {
            chanClosed();
        }
        if (len > WRITE_DIRECT_LEN) {
            // doing this instead of wrappedBuffer(buf,off,len) avoids a slice
            write(Unpooled.wrappedBuffer(buf).setIndex(off, off + len));
        } else {
            writeBuf(len).writeBytes(buf, off, len);
        }
    }

    @Override
    public int write(ByteBuffer buf) throws WTTransportException {
        if (channel == null) {
            chanClosed();
        }
        int len = buf.remaining();
        if (len > WRITE_DIRECT_LEN) {
            write(Unpooled.wrappedBuffer(buf));
        } else {
            // rewind to avoid slice allocation
            int pos = buf.position();
            try {
                writeBuf(len).writeBytes(buf);
            } finally {
                buf.position(pos);
            }
        }
        return len;
    }

    private ChannelPromise flushPromise;

    private final Runnable writeAndFlushTask = new Runnable() {
        @Override
        public void run() {
            ChannelPromise promise = flushPromise;
            if (promise == null || promise.isDone()) {
                return;
            }
            try {
                Channel chan = chan();
                int pending = pendingWrites.size();
                if (pending == 0) {
                    logger.warn("Unexpected flush with no pending bytes");
                    return;
                }
                ByteBuf last = pendingWrites.get(--pending);
                for (int i = 0; i < pending; i++) {
                    chan.write(pendingWrites.get(i), chan.voidPromise());
                    pendingWrites.set(i, null);
                }
                pendingWrites.set(pending, null);
                dataWritten = true;
                chan.writeAndFlush(last, promise);
            } catch (Exception e) {
                promise.tryFailure(e);
            }
        }
    };

    private void write(ByteBuf buf) throws WTTransportException {
        ByteBuf wbuf = writeBuf();
        int ridx = wbuf.readerIndex(), widx = wbuf.writerIndex();
        if (widx > ridx) {
            // If existing writebuf is non-empty, add its readable
            // window (duplicate backed by same data) to the pendingWrites
            ByteBuf wbDup;
            if (dupsUsed < writeBufDupPool.size()) {
                wbDup = writeBufDupPool.get(dupsUsed).setIndex(ridx, widx);
            } else {
                // read-only bytebuf has its own r/w indicies, like duplicate()
                writeBufDupPool.add(wbDup = unreleasableBuffer(wbuf.asReadOnly()));
            }
            dupsUsed++;
            wbuf.readerIndex(widx);
            pendingWrites.add(wbDup);
        }
        pendingWrites.add(buf);
        pendingBytes += buf.readableBytes();
    }

    public void writeUtf8(CharSequence seq, int knownExactByteLen) throws WTTransportException {
        if (channel == null) {
            chanClosed();
        }
        ByteBuf wbuf = writeBuf();
        final int seqLen = seq.length();
        final int wi = wbuf.writerIndex(), requiredCap = wi + knownExactByteLen;
        if (requiredCap < 4096) {
            // If required capacity is < 4KiB then just grown writebuf to accommodate
            ByteBufHelper.writeUtf8Unsafe(writeBuf(knownExactByteLen), seq, 0, seqLen, knownExactByteLen);
            return;
        }
        int cap = wbuf.capacity();
        int cindex = 0;
        // Fill/allocate new writebufs iteratively. Each time only the number of chars that
        // we *know* will fit is attempted. This is repeated while space remains in the current
        // write buffer, after which we move to a new one.
        for (int space = cap - wi; space < knownExactByteLen; ) {
            int charCount = seqLen - cindex;
            // this is based on the fact that 1 java char is between 1 and 3 utf8 bytes
            int safeCharsToWrite = Math.max(space / 3, charCount - (knownExactByteLen - space));
            if (safeCharsToWrite <= 1) {
                wbuf = allocateNewWriteBuf(Math.min(Math.max(cap, knownExactByteLen), MAX_WRITEBUF_SIZE));
                space = cap = wbuf.capacity();
            } else {
                int to = cindex + safeCharsToWrite;
                if (Character.isHighSurrogate(seq.charAt(to - 1))) {
                    to--; // we must not split a surrogate pair
                }
                int written = ByteBufHelper.writeUtf8Unsafe(wbuf, seq, cindex, to, knownExactByteLen);
                space -= written;
                knownExactByteLen -= written;
                cindex = to;
            }
        }
        int written = ByteBufHelper.writeUtf8Unsafe(wbuf, seq, cindex, seqLen, knownExactByteLen);
        assert written == knownExactByteLen;
    }

    public void writeByte(int b) throws WTTransportException {
        ByteBufHelper.writeByteUnsafe(writeBuf(1), b);
    }

    @Override
    public void flush() throws WTTransportException {
        remain();
        Channel chan = chan();
        ByteBuf wbuf = writeBuf();
        int pending = pendingWrites.size();
        if (wbuf.isReadable()) {
            ByteBuf uwbuf = unreleasableWriteBuf;
            if (uwbuf != null) {
                wbuf = uwbuf;
            } else {
                unreleasableWriteBuf = wbuf = unreleasableBuffer(wbuf);
            }
        } else if (pending > 0) {
            wbuf = null;
        } else {
            wbuf.clear();
            return; // unexpected, log error
        }
        ChannelFuture cf;
        if (pending == 0) {
            assert wbuf.isReadable();
            dataWritten = true;
            // only one buffer to write
            cf = chan.writeAndFlush(wbuf);
        } else if (pending == 1 && wbuf == null) {
            dataWritten = true;
            // only one buffer to write
            cf = chan.writeAndFlush(pendingWrites.get(0));
            pendingWrites.clear();
        } else {
            if (wbuf != null) {
                pendingWrites.add(wbuf);
            }
            cf = flushPromise = chan.newPromise();
            chan.eventLoop().execute(writeAndFlushTask);
        }
        syncChanFuture(cf, remain(), null, true);
        if ((wbuf = writeBuf) != null) {
            wbuf.clear();
        }
        pendingWrites.clear();
        pendingBytes = 0;
        dupsUsed = 0;
        flushPromise = null;
    }

    // ------------------ for tracing only ---------------

    private static final BaseEncoding hexEncoder = BaseEncoding.base16().lowerCase();

    public static CharSequence hexString(byte[] bytes, int off, int len) {
        return hexEncoder.encode(bytes, off, len);
    }


    // ----------------

//	// this one tbd
//	class FramedDecoder extends ByteToMessageDecoder {
//		private int size = -1;
//		@Override
//		protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
//			if( size == -1 ) {
//				if(in.readableBytes() >= 4) size = in.readInt();
//				else return;
//			}
//			if(in.readableBytes() >= size) out.add(in.readSlice(size).retain());
//		}
//	}

}
