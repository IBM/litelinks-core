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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.ibm.watson.litelinks.server.ReleaseAfterResponse;
import com.ibm.watson.litelinks.server.RequestListener;
import com.ibm.watson.litelinks.server.ServerRequestThread;
import com.ibm.watson.litelinks.server.TTransportParameters;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TBase;
import org.apache.thrift.TBaseProcessor;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolDecorator;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Litelinks-specific "extensions" to the thrift protocol. Clients
 * will only use these when the server has advertised support.
 */
public class LitelinksTProtoExtension {

    private static final Logger logger = LoggerFactory.getLogger(LitelinksTProtoExtension.class);

    public static final String HEADER = "#", PING = "#P";

    private static final byte INCL_CUSTOM = 0x01, INCL_MDC = 0x02, INCL_BOTH = INCL_CUSTOM | INCL_MDC;

    private static final String TIMEOUT_TC_KEY = "_ll.timeout";

    private LitelinksTProtoExtension() {}

    public static class ProcessorFactory extends TProcessorFactory {
        private final RequestListener[] listeners;
        public ProcessorFactory(TProcessor delegate, RequestListener[] listeners) {
            super(delegate);
            this.listeners = listeners;
        }
        @Override
        public TProcessor getProcessor(TTransport trans) {
            return new Processor(super.getProcessor(trans), listeners);
        }
        @Override
        public boolean isAsyncProcessor() {
            return false;
        }
    }

    /**
     * NOT threadsafe
     */
    public static class Processor implements TProcessor {

        private final TProcessor delegate;
        private InterceptTInProto intercept;
        private boolean clearThreadLocals = true;

        private final RequestListener[] listeners;

        public Processor(TProcessor delegate, RequestListener[] listeners) {
            this.delegate = delegate;
            this.listeners = listeners;
        }

        public void setClearThreadLocals(boolean clearThreadLocals) {
            this.clearThreadLocals = clearThreadLocals;
        }

        @Override
        public void process(TProtocol in, TProtocol out) throws TException {
            TMessage msg = in.readMessageBegin();
            try {
                Map<String, String> cxt = null;
                if (HEADER.equals(msg.name)) {
                    // Process special header containing litelinks ThreadContext
                    // and/or logger MDC data (also called ThreadContext in log4j2)

                    String realName = in.readString();
                    byte incl = in.readByte(); // one-byte manifest
                    if ((incl & INCL_CUSTOM) != 0) {
                        int msize = in.readI16();
                        long timeoutNanos = -1L;
                        if (msize > 0) {
                            Builder<String, String> imb = ImmutableMap.builder();
                            for (int i = 0; i < msize; i++) {
                                imb.put(in.readString(), in.readString());
                            }
                            cxt = imb.build();
                            String timeoutVal = cxt.get(TIMEOUT_TC_KEY);
                            if (timeoutVal != null) {
                                try {
                                    timeoutNanos = Long.parseLong(timeoutVal, 16);
                                } catch (NumberFormatException nfe) {
                                    logger.warn("Ignoring invalid value for litelinks "
                                                + TIMEOUT_TC_KEY + " ThreadContext param: " + timeoutVal);
                                }
                            }
                        } else {
                            cxt = Collections.emptyMap();
                        }
                        ThreadContext.setCurrentContext(cxt, timeoutNanos);
                    }
                    if ((incl & INCL_MDC) != 0) {
                        int msize = in.readI16();
                        for (int i = 0; i < msize; i++) {
                            MDC.put(in.readString(), in.readString());
                        }
                    }
                    msg = new TMessage(realName, msg.type, msg.seqid);
                }
                //NOTE no "else" here because may have both HEADER + PING
                if (PING.equals(msg.name)) {
                    //assert msg.type == TMessageType.CALL;
                    in.readStructBegin();
                    in.readFieldBegin();
                    in.readStructEnd();
                    in.readMessageEnd();
                    out.writeMessageBegin(new TMessage(PING, TMessageType.REPLY, msg.seqid));
                    out.writeMessageEnd();
                    out.getTransport().flush();
                    return;
                }

                Thread curThread = Thread.currentThread();
                if (curThread instanceof ServerRequestThread) {
                    ((ServerRequestThread) curThread).setMethodName(msg.name);
                }

                if (intercept == null || intercept.delegate != in) {
                    Map<String, Object> transParams = listeners != null && listeners.length > 0
                            ? TTransportParameters.getParameterMap(in.getTransport()) : null;
                    intercept = new InterceptTInProto(in, msg, transParams);
                } else {
                    intercept.setMsg(msg);
                }

                Object[] handles = null;
                int listenerCount = 0;
                Exception listenerErr = null;
                try {
                    // call newRequest method on any RequestListeners
                    if (listeners != null) {
                        for (int i = 0; i < listeners.length; i++) {
                            RequestListener rl = listeners[i];
                            Object h;
                            try {
                                h = rl.newRequest(msg.name, cxt, intercept.transParams);
                                listenerCount++;
                            } catch (Exception e) {
                                listenerErr = e;
                                out.writeMessageBegin(new TMessage(msg.name, TMessageType.EXCEPTION, msg.seqid));
                                if (e instanceof TException && e instanceof TBase) {
                                    ((TBase<?, ?>) e).write(out);
                                } else {
                                    new TApplicationException(TApplicationException.INTERNAL_ERROR,
                                            String.valueOf(e)).write(out);
                                }
                                out.writeMessageEnd();
                                out.getTransport().flush();
                                // read remainder of input message
                                if (delegate instanceof TBaseProcessor) {
                                    ((TBaseProcessor<?>) delegate).getProcessMapView()
                                            .get(msg.name).getEmptyArgsInstance().read(in);
                                }
                                in.readMessageEnd();
                                return;
                            }
                            if (h != null) {
                                if (handles == null) {
                                    handles = new Object[listeners.length];
                                }
                                handles[i] = h;
                            }
                        }
                    }
                    in = intercept;
                    delegate.process(in, out);
                } finally {
                    // call requestComplete method on any RequestListeners
                    if (listenerCount > 0) {
                        for (int i = listenerCount - 1; i >= 0; i--) {
                            try {
                                listeners[i].requestComplete(msg.name, cxt, intercept.transParams,
                                        listenerErr, handles != null? handles[i] : null);
                            } catch (RuntimeException e) {
                                logger.warn("requestComplete method of listener "
                                            + listeners[i].getClass().getName() + " threw exception", e);
                            }
                        }
                    }
                }
            } finally {
                // *always* cleanup the ThreadContext/MDC (even it they weren't sent in
                // the latest request, they could have been set by the service impl)
                if (clearThreadLocals) {
                    clearThreadLocals();
                }
                // else the TServer impl will call this after returning the response,
                // so as not to affect latency (e.g. in NettyTServer.ProcessTask#run())
            }
        }

        public static void clearThreadLocals() {
            ReleaseAfterResponse.releaseAll();
            ThreadContext.removeCurrentContext();
            MDC.clear();
        }
    }

    public static class SafeBinaryTProtocol extends TProtocolDecorator {
        public SafeBinaryTProtocol(TProtocol delegate) {
            super(delegate);
        }
        @Override
        public void writeBinary(ByteBuffer buf) throws TException {
            try {
                super.writeBinary(buf);
            } catch (UnsupportedOperationException ioe) {
                // must provide array-accessible copy
                super.writeBinary((ByteBuffer) ByteBuffer
                        .allocate(buf.remaining()).put(buf).flip());
            }
        }
    }

    // only used on server side
    static class InterceptTInProto extends TProtocolDecorator {
        final TProtocol delegate;
        final Map<String, Object> transParams;
        final NettyTTransport ntt;
        private TMessage msg;

        public InterceptTInProto(TProtocol delegate, TMessage msg,
                Map<String, Object> transParams) {
            super(delegate);
            this.delegate = delegate;
            this.msg = msg;
            this.transParams = transParams;
            TTransport tt = delegate.getTransport();
            ntt = tt instanceof NettyTTransport?
                    (NettyTTransport) tt : null;
        }
        public void setMsg(TMessage msg) {
            this.msg = msg;
        }
        @Override
        public TMessage readMessageBegin() throws TException {
            return msg;
        }
        @Override
        public void readMessageEnd() throws TException {
            delegate.readMessageEnd();
            if (ntt != null) {
                ntt.startIOTimer(0); // cancel read timeout
            }
        }

    }

    // only used on client side
    public static class InterceptTOutProto extends TProtocolDecorator {
        final TProtocol delegate;
        final boolean mdc;

        public InterceptTOutProto(TProtocol delegate, boolean mdc) {
            super(delegate);
            this.delegate = delegate;
            this.mdc = mdc;
        }
        @Override
        public void writeMessageBegin(TMessage tMessage) throws TException {
            Map<String, String> cxtMap = ThreadContext.getCurrentContext();
            long timeoutNanos = ThreadContext.nanosUntilDeadline();
            Map<?, ?> mdc = this.mdc? MDC.getCopyOfContextMap() : null;
            boolean inclMdc = mdc != null && !mdc.isEmpty();
            boolean inclCxt = cxtMap != null || timeoutNanos != -1L;
            if (!inclCxt && !inclMdc) {
                delegate.writeMessageBegin(tMessage);
                return;
            }
            TMessage msg = new TMessage(HEADER, tMessage.type, tMessage.seqid);
            delegate.writeMessageBegin(msg);
            delegate.writeString(tMessage.name);
            // one-byte manifest
            delegate.writeByte(inclMdc? inclCxt? INCL_BOTH : INCL_MDC : INCL_CUSTOM);
            if (inclCxt) {
                writeContextMap(delegate, cxtMap, timeoutNanos);
            }
            if (inclMdc) {
                writeMap(delegate, mdc);
            }
        }
    }

    private static void writeMap(TProtocol out, Map<?, ?> map) throws TException {
        out.writeI16((short) map.size());
        for (Map.Entry<?, ?> ent : map.entrySet()) {
            out.writeString(String.valueOf(ent.getKey()));
            out.writeString(String.valueOf(ent.getValue()));
        }
    }

    private static void writeContextMap(TProtocol out,
            Map<?, ?> map, long timeoutNanos) throws TException {
        int size = map != null? map.size() : 0;
        boolean skipDeadlineEntry = false;
        if (timeoutNanos == -1L) {
            out.writeI16((short) size);
        } else {
            if (map != null && map.containsKey(TIMEOUT_TC_KEY)) {
                out.writeI16((short) size);
                skipDeadlineEntry = true;
            } else {
                out.writeI16((short) (size + 1));
            }
            out.writeString(TIMEOUT_TC_KEY);
            out.writeString(Long.toHexString(timeoutNanos));
        }
        if (map != null) {
            for (Map.Entry<?, ?> ent : map.entrySet()) {
                Object key = ent.getKey();
                if (skipDeadlineEntry && TIMEOUT_TC_KEY.equals(key)) {
                    continue;
                }
                out.writeString(String.valueOf(key));
                out.writeString(String.valueOf(ent.getValue()));
            }
        }
    }

    private static final TStruct EMPTY_STRUCT = new TStruct();

    public static void ping(TProtocol in, TProtocol out) throws TException {
        TMessage msg = new TMessage(PING, TMessageType.CALL,
                ThreadLocalRandom.current().nextInt()); //TODO seq num TBD
        out.writeMessageBegin(msg);
        out.writeStructBegin(EMPTY_STRUCT);
        out.writeFieldStop();
        out.writeStructEnd();
        out.writeMessageEnd();
        out.getTransport().flush();
        TMessage msgin = in.readMessageBegin();
        TApplicationException tae = null;
        if (msgin.type == TMessageType.EXCEPTION) {
            tae = TApplicationException.readFrom(in);
        }
        in.readMessageEnd();
        if (tae != null && tae.getType() != TApplicationException.UNKNOWN_METHOD) {
            throw tae;
        }
        if (msgin.seqid != msg.seqid || !PING.equals(msgin.name)) {
            throw new TException("Unexpected ping response");
        }
    }

    // check that the litelinks-modified version of the TCompactProtocol class is loaded
    // and if not, don't use our optimized TDCompactProtocol subclass
    private static final boolean LL_TCOMPACT_CLASS;

    static {
        boolean found = false;
        try {
            found = TCompactProtocol.class.getField("LITELINKS_MOD") != null;
        } catch (NoSuchFieldException nsfe) {
        }
        LL_TCOMPACT_CLASS = found;
        if (!found) {
            logger.warn("Litelinks-modified TCompactProtocol class NOT loaded,"
                        + " ensure litelinks-core jar is before libthrift on the classpath");
        }
    }

    public static TProtocolFactory getOptimizedTProtoFactory(TProtocolFactory tpf) {
        if (LL_TCOMPACT_CLASS && tpf instanceof TCompactProtocol.Factory) {
            return TDCompactProtocol.FACTORY;
        }
        if (tpf instanceof TBinaryProtocol.Factory) {
            return TDBinaryProtocol.FACTORY;
        }
        return trans -> new SafeBinaryTProtocol(tpf.getProtocol(trans));
    }
}
