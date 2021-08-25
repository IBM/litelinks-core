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

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.Service;
import com.ibm.watson.litelinks.LitelinksSystemPropNames;
import com.ibm.watson.litelinks.LitelinksTProtoExtension;
import com.ibm.watson.litelinks.MethodInfo;
import com.ibm.watson.litelinks.SSLHelper;
import org.apache.thrift.TBaseProcessor;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static com.ibm.watson.litelinks.ThriftConnProp.*;
import static com.ibm.watson.litelinks.server.AdapterThriftService.getIfaceFromSvcClass;

public class DefaultThriftServer extends AbstractService
        implements ListeningService, NamedService, ConfiguredService, DeployedService {

    private static final Logger logger = LoggerFactory.getLogger(DefaultThriftServer.class);

    public static final TProtocolFactory DEFAULT_TPROTOFAC = new TCompactProtocol.Factory();

    // this must be true from now on since it handles the
    // request listener invocations
    public static final boolean LITELINKS_PROTO_EXTENSIONS = true;

    public static final boolean FORCE_HOMOGENEOUS_CONN_PROPS
            = "true".equals(System.getProperty(LitelinksSystemPropNames.FORCE_HOMOG_CONN_PROPS));

    // ensure shutdown timeouts are long enough that they won't expire
    // before enclosing timeout
    public static final int SERVER_SHUTDOWN_TIMEOUT_SECS =
            (int) (LitelinksService.getMaxShutdownTimeoutMs() / 1000) + 1;
    public static final int SVC_IMPL_SHUTDOWN_TIMEOUT_SECS =
            (int) (LitelinksService.getMaxShutdownTimeoutMs() / 1000) + 1;

    public static final int DEFAULT_STARTUP_TIMEOUT_SECS = 360; // 6mins

    public static final boolean DEFAULT_FRAMED = false;

    public enum SSLMode {NONE, ENABLED, CLIENT_AUTH}

    private final int startupTimeoutSecs;

    private final Class<? extends ThriftService> tsClass;
    private ThriftService ts;
    private /*final*/ TProcessor tp;
    private final TProtocolFactory tpf;
    private final SSLMode sslMode;
    private final boolean framed = DEFAULT_FRAMED; //TODO

    private final InetSocketAddress specifiedAddr;
    private TServer selectorServer;

    private SettableServiceDeploymentInfo deploymentInfo;
    private final RequestListener[] reqListeners;

    private final ScheduledExecutorService serviceEventThreads;
//  private Thread serverThread;

    private volatile Thread currentAppThread; // just for timeout logging

    private long implStartTime;

    // for timeouts stopping/starting the thrift server (not service impl)
    private enum TServerState {STARTED, STOPPED, TIMED_OUT}

    private final AtomicReference<TServerState> timeoutState = new AtomicReference<>();

    public DefaultThriftServer(TProcessor tp, InetSocketAddress bindAddress, TProtocolFactory tpf,
            SSLMode sslMode, int startupTimeoutSecs, RequestListener[] reqListeners) {
        this((Class<ThriftService>) null, bindAddress, tpf, sslMode, startupTimeoutSecs, reqListeners);
        this.tp = tp;
    }

    public DefaultThriftServer(ThriftService ts, InetSocketAddress bindAddress,
            TProtocolFactory tpf, SSLMode sslMode) {
        this((Class<ThriftService>) null, bindAddress, tpf, sslMode);
        this.ts = ts;
    }

    public DefaultThriftServer(ThriftService ts, InetSocketAddress bindAddress, TProtocolFactory tpf,
            SSLMode sslMode, int startupTimeoutSecs, RequestListener[] reqListeners) {
        this((Class<ThriftService>) null, bindAddress, tpf, sslMode, startupTimeoutSecs, reqListeners);
        this.ts = ts;
    }

    public DefaultThriftServer(Class<? extends ThriftService> tsClass, InetSocketAddress bindAddress,
            TProtocolFactory tpf, SSLMode sslMode) {
        this(tsClass, bindAddress, tpf, sslMode, DEFAULT_STARTUP_TIMEOUT_SECS, null);
    }

    public DefaultThriftServer(Class<? extends ThriftService> tsClass, InetSocketAddress bindAddress,
            TProtocolFactory tpf, SSLMode sslMode, int startupTimeoutSecs,
            RequestListener[] reqListeners) {
        this.tsClass = tsClass;
        this.specifiedAddr = bindAddress;
        this.tpf = tpf != null ? tpf : DEFAULT_TPROTOFAC;
        this.sslMode = sslMode == null ? SSLMode.NONE : sslMode;
        this.serviceEventThreads = LitelinksService.getServiceEventThreads();
        this.startupTimeoutSecs = startupTimeoutSecs;
        this.reqListeners = reqListeners;
    }

    @Override
    public void setDeploymentInfo(SettableServiceDeploymentInfo deploymentInfo) {
        this.deploymentInfo = deploymentInfo;
        deploymentInfo.setListeningAddress(specifiedAddr);
    }

    @Override
    public SocketAddress getListeningAddress() {
        return deploymentInfo.getListeningAddress();
    }

    @Override
    public String getServiceName() {
        return getServiceName(ts, tp, tsClass);
    }

    private static String getServiceName(ThriftService ts, TProcessor tp,
            Class<? extends ThriftService> tsClass) {
        String name = ts != null ? ts.defaultServiceName() : null;
        if (name == null) {
            if (tp != null) {
                Class<?> encClass = tp.getClass().getEnclosingClass();
                if (encClass != null) {
                    name = encClass.getName();
                }
            }
            if (name == null && tsClass != null) {
                name = tsClass.getName();
            }
        }
        return name;
    }

    @Override
    public String getServiceVersion() {
        ThriftService ts = this.ts;
        return ts != null ? ts.serviceVersion() : null;
    }

    public TProtocolFactory getProtocolFactory() {
        return tpf;
    }

    public SSLMode getSSLMode() {
        return sslMode;
    }

    public boolean isFramedTransport() {
        return framed;
    }

    @Override
    public Map<String, String> getConfig() throws Exception {
        SSLMode sm = this.sslMode;
        TProcessor tp = this.tp;
        Class<?> tpc = tp != null ? tp.getClass() : null;
        String scName = TMultiplexedProcessor.class.equals(tpc) ? MULTIPLEX_CLASS
                : tpc != null && tpc.getDeclaringClass() != null ? tpc.getDeclaringClass().getName() : null;
        ImmutableMap.Builder<String, String> bld = new ImmutableMap.Builder<String, String>()
                .put(TR_PROTO_FACTORY, tpf.getClass().getName())
                .put(TR_FRAMED, Boolean.toString(framed))
                .put(TR_SSL, Boolean.toString(sm == SSLMode.ENABLED || sm == SSLMode.CLIENT_AUTH))
                .put(TR_EXTRA_INFO, String.valueOf(LITELINKS_PROTO_EXTENSIONS));
        if (scName != null) {
            bld.put(SERVICE_CLASS, scName);
        }
        ThriftService ts = this.ts;
        Map<String, MethodInfo> methodInfos = getMethodInfos(tp, ts);
        if (methodInfos != null) {
            for (Map.Entry<String, MethodInfo> ent : methodInfos.entrySet()) {
                MethodInfo mi = ent.getValue();
                if (mi != null) {
                    bld.put(METH_INFO_PREFIX + ent.getKey(), mi.serialize());
                }
            }
        }
        if (ts != null) {
            Map<String, String> metadata = ts.provideInstanceMetadata();
            if (metadata != null) {
                for (Map.Entry<String, String> ent : metadata.entrySet()) {
                    bld.put(APP_METADATA_PREFIX + ent.getKey(), ent.getValue());
                }
            }
        }
        return bld.build();
    }

    private static Map<String, MethodInfo> getMethodInfos(TProcessor tp, ThriftService ts) throws Exception {
        if(tp == null) return null;
        Class<?> sc = tp.getClass().getDeclaringClass();
        if(sc == null) return null;
        Class<?> iface = getIfaceFromSvcClass(sc);
        if(iface == null) return null;
        Map<String, MethodInfo> providedInfos = ts != null ? ts.provideMethodInfo() : null;
        Class<?> implClass = null;
        if (tp instanceof TBaseProcessor) {
            try {
                Object impl = ifaceField.get(tp);
                if (impl != null) {
                    implClass = impl.getClass();
                }
            } catch (IllegalArgumentException | IllegalAccessException e) {}
        }
        if (implClass == null && (providedInfos == null || providedInfos.isEmpty())) {
            return null;
        }
        Map<String, MethodInfo> infosCopy = providedInfos == null || providedInfos.isEmpty() ?
                null : new HashMap<>(providedInfos);

        ImmutableMap.Builder<String, MethodInfo> bld = new ImmutableMap.Builder<String, MethodInfo>();
        for (Method ifaceMeth : iface.getMethods()) {
            String methName = ifaceMeth.getName();
            MethodInfo providedInfo = infosCopy != null ? infosCopy.remove(methName) : null;
            // provided takes precedence
            if (providedInfo != null) {
                bld.put(methName, providedInfo);
            }
            // then look for annotations
            else if (implClass != null) {
                try {
                    Method implMeth = implClass.getMethod(methName, ifaceMeth.getParameterTypes());
                    if (implMeth != null) {
                        boolean idempotent = implMeth.getAnnotation(Idempotent.class) != null;
                        InstanceFailures instFailAnnot = implMeth.getAnnotation(InstanceFailures.class);
                        Class<? extends Exception>[] instFailTypes = instFailAnnot != null
                                ? instFailAnnot.value() : null;
                        Set<Class<? extends Exception>> instFailTypesSet =
                                instFailTypes == null || instFailTypes.length == 0 ?
                                        null : new HashSet<>(Arrays.asList(instFailTypes));
                        if (idempotent || instFailTypes != null && instFailTypes.length > 0) {
                            bld.put(methName, MethodInfo.builder().setIdempotent(idempotent)
                                    .setInstanceFailureExceptions(instFailTypesSet).build());
                        }
                    }
                } catch (NoSuchMethodException nsme) {}
            }
        }
        MethodInfo defaultMi = infosCopy != null ? infosCopy.remove(MethodInfo.DEFAULT) : null;
        if (defaultMi != null) {
            bld.put(MethodInfo.DEFAULT, defaultMi);
        }

        if (infosCopy != null && !infosCopy.isEmpty()) {
            throw new Exception("MethodInfo provided for method(s) that don't belong to"
                                + " the service interface " + iface + ": " + infosCopy.keySet());
        }
        return bld.build();
    }

    static final Field ifaceField;

    static {
        try {
            ifaceField = TBaseProcessor.class.getDeclaredField("iface");
            ifaceField.setAccessible(true);
        } catch (NoSuchFieldException | SecurityException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void verifyConfig(Map<String, String> other) throws ConfigMismatchException {
        TProcessor tp = this.tp;
        if (tp != null) {
            Object otherScName = other.get(SERVICE_CLASS);
            if (otherScName != null) {
                Class<?> sc = tp.getClass().getDeclaringClass();
                if (otherScName != null && sc != null && !otherScName.equals(sc.getName())) {
                    Class<?> serviceIface = getIfaceFromSvcClass(sc), otherIface = null;
                    if (serviceIface != null) {
                        try {
                            otherIface = getIfaceFromSvcClass(Class.forName((String) otherScName));
                        } catch (ClassNotFoundException cnfe) {}
                    }
                    if (otherIface == null || !otherIface.isAssignableFrom(serviceIface)) {
                        throw new ConfigMismatchException("service interface mismatch");
                    }
                }
            }
        }
        String msg = null;
        if (!tpf.getClass().getName().equals(other.get(TR_PROTO_FACTORY))) {
            msg = "protocol mismatch";
        } else if (!Boolean.toString(framed).equals(other.get(TR_FRAMED))) {
            msg = "framed transport mismatch";
        } else if (!Boolean.toString(sslMode == SSLMode.ENABLED || sslMode == SSLMode.CLIENT_AUTH)
                .equals(other.get(TR_SSL))) {
            msg = "ssl setting mismatch";
        }
        if (msg != null) {
            if (FORCE_HOMOGENEOUS_CONN_PROPS) {
                throw new ConfigMismatchException(msg);
            }
            logger.warn("Connection config of new instance does not match others in cluster: " + msg);
        }
    }

    @Override
    protected void doStart() {
        //do all the work on a separate thread
        final TProcessor tp = this.tp;
        if (tsClass == null && tp == null && ts == null) {
            notifyFailed(new IllegalStateException("No service implementation to run"));
            return;
        }
        if (deploymentInfo == null) {
            notifyFailed(new IllegalStateException("No ServiceDeploymentInfo provided"));
            return;
        }

        // ensure we start within the timeout, fail if not
        final ScheduledFuture<?> timeoutFuture = serviceEventThreads.schedule(this::startupTimeoutFired,
                startupTimeoutSecs, TimeUnit.SECONDS);

        //now we're ready so start it up in a separate thread
        serviceEventThreads.execute(() -> {
            currentAppThread = Thread.currentThread();
            try {
                if (ts == null && tsClass == null) {
                    assert tp != null;
                    startThriftServer(tp, null, timeoutFuture);
                } else {
                    logger.info("initializing service implementation...");
                    implStartTime = System.currentTimeMillis();
                    final ThriftService ts;
                    if (this.ts == null) {
                        this.ts = ts = tsClass.newInstance();
                    } else {
                        ts = this.ts;
                    }
                    ts.setDeploymentInfo(deploymentInfo);
                    ts.addListener(serviceImplListener(timeoutFuture), serviceEventThreads);
                    ts.startAsync();
                }
            } catch (Throwable t) {
                notifyFailed(t);
            } finally {
                currentAppThread = null;
            }
        });
    }

    private Listener serviceImplListener(final ScheduledFuture<?> timeoutFuture) {
        return new Listener() {
            @Override
            public void running() {
                long took = System.currentTimeMillis() - implStartTime;
                String time = took < 30000 ? took + "ms" : took / 1000 + "s";
                logger.info("service implementation initialization complete, took " + time);
                final ThriftService ts = DefaultThriftServer.this.ts;
                final TProcessor tp = ts.getTProcessor();
                if (deploymentInfo.getServiceName() == null) {
                    deploymentInfo.setServiceName(getServiceName(ts, tp, tsClass));
                }
                if (deploymentInfo.getServiceVersion() == null) {
                    deploymentInfo.setServiceVersion(ts.serviceVersion());
                }
                Throwable failure = null;
                if (tp != null) {
                    try {
                        DefaultThriftServer.this.tp = tp;
                        ExecutorService userThreads = ts.provideRequestExecutor();
                        startThriftServer(tp, userThreads, timeoutFuture);
                    } catch (Throwable te) {
                        failure = te;
                    }
                } else {
                    failure = new IllegalStateException(tsClass.getName() + " did not provide a TProcessor");
                }
                if (failure != null) {
                    try {
                        // attempt shutdown
                        timeoutFuture.cancel(false);
                        shutdownServiceSync();
                    } catch (Throwable t) {
                        logger.warn("Exception shutting down service after failure", t);
                    } finally {
                        notifyFailed(failure);
                    }
                }
            }

            @Override
            public void failed(Service.State from, Throwable failure) {
                timeoutFuture.cancel(false);
                if (from == Service.State.STARTING) {
                    logger.error("service implementation initialization failed", failure);
                    assert selectorServer == null;
                    notifyFailed(failure);
                } else if (from == Service.State.RUNNING) {
                    //TODO maybe store pendingfailure
                    stopAsync(); // will end up in failed state
                }
            }
        };
    }


    private void startThriftServer(TProcessor tp, ExecutorService appThreads, final ScheduledFuture<?> timeoutFuture)
            throws IOException, GeneralSecurityException {
        int requestThreads = getNumRequestThreads();
        logger.info("Thrift server max # request threads = " + (requestThreads != -1 ? requestThreads : "unlimited"));
        if (appThreads != null) {
            logger.info(
                    "Using service-impl-provided threadpool for requests, max # worker threads reported above won't apply");
        }
        TThreadedSelectorServer.Args args =
                (TThreadedSelectorServer.Args) new TThreadedSelectorServer.Args(specifiedAddr)
                        .protocolFactory(LitelinksTProtoExtension.getOptimizedTProtoFactory(tpf))
                        .workerThreads(requestThreads)
                        .executorService(appThreads) // typically null
                        .stopTimeoutUnit(TimeUnit.SECONDS).stopTimeoutVal(SERVER_SHUTDOWN_TIMEOUT_SECS);
        if (!framed) {
            args.transportFactory(null);
        }
        if (!LITELINKS_PROTO_EXTENSIONS) {
            args.processor(tp);
        } else {
            args.processorFactory(new LitelinksTProtoExtension.ProcessorFactory(tp, reqListeners));
        }

        // retrieve SSL parameters if configured
        if (sslMode == SSLMode.ENABLED || sslMode == SSLMode.CLIENT_AUTH) {
            args.sslContext = SSLHelper.getSslContext(null, true, sslMode == SSLMode.CLIENT_AUTH);
        }

//      selectorServer = new TThreadedSelectorServer(args);
        selectorServer = new NettyTServer(args);

        // register listener with thrift server to be notified when it is ready
        // to accept incoming connections
        selectorServer.setServerEventHandler(tServerEventHandler(timeoutFuture));

        if (timeoutState.get() != null) {
            return;
        }
        logger.info("starting thrift server endpoint");

        /*serverThread =*/
        new Thread("ll-thrift-serving-thread") {
            { setDaemon(true); }

            @Override
            public void run() {
                if (timeoutState.get() != null) {
                    return;
                }
                try {
                    selectorServer.serve(); // this blocks until the server is stopped
                } catch (Throwable t) {
                    if (timeoutState.get() != TServerState.TIMED_OUT) {
                        notifyFailed(t);
                    }
                    if (t instanceof Error) throw t;
                    return;
                }
                // thrift server stopped cleanly
                if (timeoutState.compareAndSet(TServerState.STARTED, TServerState.STOPPED)) {
                    try {
                        if (tstart != 0) {
                            logger.info("thrift server took " + (System.currentTimeMillis() - tstart) + "ms to stop");
                        } else {
                            logger.warn("thrift server stopped unexpectedly");
                        }
                        deploymentInfo.setListeningAddress(specifiedAddr);
                        shutdownServiceSync(); //TODO TBD if this stays in this thread
                        notifyStopped();
                    } catch (Throwable t) {
                        notifyFailed(t);
                        if (t instanceof Error) throw (Error)t;
                    }
                }
            }
        }.start();
    }

    /**
     * @see LitelinksSystemPropNames#NUM_PROCESS_THREADS
     */
    private static int getNumRequestThreads() throws IllegalArgumentException {
        int numThreads;
        String argVal = System.getProperty(LitelinksSystemPropNames.NUM_PROCESS_THREADS, "unlimited");
        try {
            if ("unlimited".equals(argVal) || "-1".equals(argVal)) {
                numThreads = -1;
            } else if (argVal.endsWith("c")) {
                double mult = Double.parseDouble(argVal.substring(0, argVal.length() - 1));
                numThreads = (int) Math.ceil(mult * Runtime.getRuntime().availableProcessors());
            } else {
                numThreads = Integer.parseInt(argVal);
                if (numThreads <= 0) {
                    throw new IllegalArgumentException();
                }
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid number of proc threads specified: " + argVal);
        }
        return numThreads;
    }

    private TServerEventHandler tServerEventHandler(final ScheduledFuture<?> timeoutFuture) {
        return new TServerEventHandler() {
            @Override
            public void preServe() {
                logger.info("thrift server " + selectorServer + " about to serve");
                if (timeoutState.get() != null) {
                    return;
                }
                if (selectorServer instanceof ListeningService) {
                    deploymentInfo.setListeningAddress(
                            ((ListeningService) selectorServer).getListeningAddress());
                }
                // initialize any RequestListeners before returning (must complete
                // prior to processing any incoming requests)
                if (reqListeners != null) {
                    for (RequestListener rl : reqListeners) {
                        try {
                            logger.info("Initializing RequestListener: " + rl.getClass().getName());
                            rl.initialize(deploymentInfo);
                        } catch (Exception e) {
                            logger.error("RequestListener init failed for " + rl.getClass().getName()
                                         + ", aborting startup", e);
                            selectorServer.stop(); // (async)
                            notifyFailed(e);
                            return;
                        }
                    }
                }

                // check whether we already timed out, and if so don't proceed
                if (!timeoutState.compareAndSet(null, TServerState.STARTED)) {
                    // stop() will have been called when the timeout happened,
                    // but call it again now just incase
                    selectorServer.stop(); // (async)
                } else {
                    if (timeoutFuture != null) {
                        timeoutFuture.cancel(false);
                    }
                    logger.info("thrift server listening on " + deploymentInfo.getListeningAddress());
                    notifyStarted();
                }
            }

            @Override public ServerContext createContext(TProtocol i, TProtocol o) {return null;}
            @Override public void processContext(ServerContext sc, TTransport it, TTransport ot) {}
            @Override public void deleteContext(ServerContext sc, TProtocol i, TProtocol o) {}
        };

    }

    private void startupTimeoutFired() {
        Throwable threadStack = null;
        if (state() == State.STARTING && timeoutState.compareAndSet(null, TServerState.TIMED_OUT)) {
            try {
                ThriftService ts = this.ts;
                Thread curThread = currentAppThread;
                if (curThread != null) {
                    threadStack = new Throwable("Point-in-time stacktrace of timed-out service impl initialization:");
                    threadStack.setStackTrace(curThread.getStackTrace());
                }
                if (ts.state() == State.STARTING) {
                    logger.warn("service implementation initialization timed out");
                } else {
                    logger.warn("timeout during thrift server start");
                    TServer ss = selectorServer;
                    if (ss != null) {
                        ss.stop(); // (async)
                    }
                    shutdownServiceSync();
                    if (ss != null || (ss = selectorServer) != null) {
                        ss.stop();
                    }
                }
            } catch (Throwable t) {
                logger.warn("Exception shutting down service after start timeout", t);
            } finally {
                Exception te = new TimeoutException("the service did not start within "
                                                    + startupTimeoutSecs + " seconds");
                if (threadStack != null) {
                    te.initCause(threadStack);
                }
                notifyFailed(te);
            }
        }
    }

    @Override
    public void preShutdown() {
        final ThriftService ts = this.ts;
        if (ts != null) {
            ts.preShutdown();
        }
    }

    @Override
    public boolean isLive() {
        final ThriftService ts = this.ts;
        return ts == null || ts.isLive();
//     return ts != null ? ts.isLive() : selectorServer != null
//            && selectorServer.isServing();
    }

    @Override
    public boolean isReady() {
        final ThriftService ts = this.ts;
        return ts == null || ts.isReady();
    }

    // synchronous; assumes state is at least running
    private void shutdownServiceSync() throws Throwable {
        final ThriftService ts = this.ts;
        if (ts == null) {
            return;
        }
//      assert ts.state() >= RUNNING
        logger.info("stopping service implementation...");
        // even though this should be 'async' we defensively
        // assume the service isn't implemented this way
        serviceEventThreads.execute(ts::stopAsync);
        Future<?>[] futs = null;
        if (reqListeners != null && reqListeners.length > 0) {
            futs = new Future[reqListeners.length];
            for (int i = 0; i < reqListeners.length; i++) {
                final RequestListener rl = reqListeners[i];
                futs[i] = serviceEventThreads.submit(() -> {
                    try {
                        rl.shutdown();
                    } catch (RuntimeException e) {
                        logger.warn("RequestListener " + rl.getClass().getName()
                                    + " threw Exception during shutdown", e);
                    }
                });
            }
        }
        try {
            long before = System.nanoTime();
            ts.awaitTerminated(SVC_IMPL_SHUTDOWN_TIMEOUT_SECS, TimeUnit.SECONDS);
            logger.info("service implementation stopped");
            if (futs != null) {
                long deadline = before + TimeUnit.NANOSECONDS
                        .convert(SVC_IMPL_SHUTDOWN_TIMEOUT_SECS, TimeUnit.SECONDS);
                for (int i = 0; i < futs.length; i++) {
                    long rem = deadline - System.nanoTime();
                    if (rem <= 0) {
                        throw new TimeoutException();
                    }
                    futs[i].get(rem, TimeUnit.NANOSECONDS);
                }
            }
        } catch (IllegalStateException e) {
            Throwable t = ts.failureCause();
            throw t != null ? t : e;
        }
    }

    long tstart;

    @Override
    protected void doStop() {
        final TServer ss = selectorServer;
        if (ss != null) {
            logger.info("stopping thrift server...");
            tstart = System.currentTimeMillis();
            ss.stop(); // this is asynchronous
        } else {
            serviceEventThreads.execute(() -> {
                try {
                    shutdownServiceSync();
                    notifyStopped();
                } catch (Throwable t) {
                    notifyFailed(t);
                }
            });
        }
    }
}
