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
package com.ibm.watson.litelinks.client;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.ibm.watson.litelinks.LitelinksExceptions;
import com.ibm.watson.litelinks.LitelinksSystemPropNames;
import com.ibm.watson.litelinks.MethodInfo;
import com.ibm.watson.litelinks.NettyTTransport;
import com.ibm.watson.litelinks.ThreadContext;
import com.ibm.watson.litelinks.WTTransportException;
import com.ibm.watson.litelinks.client.FallbackProvider.FailureInformation;
import com.ibm.watson.litelinks.client.FallbackProvider.FailureInformation.Cause;
import com.ibm.watson.litelinks.client.ServiceInstance.FailType;
import com.ibm.watson.litelinks.client.ServiceInstanceCache.Balancers;
import com.ibm.watson.litelinks.client.ThriftClientBuilder.MethodConfig;
import com.ibm.watson.litelinks.server.ReleaseAfterResponse;
import io.netty.util.concurrent.FastThreadLocalThread;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.SocketException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.ibm.watson.litelinks.LitelinksExceptions.isInterruption;
import static com.ibm.watson.litelinks.TDCompactProtocol.POOLED_BUFS;
import static java.lang.System.nanoTime;

/**
 * Performs the RPC method invocations
 */
class ClientInvocationHandler<C extends TServiceClient> implements InvocationHandler {
    private static final Logger logger = LoggerFactory.getLogger(ClientInvocationHandler.class);

    public static final int MAX_RETRIES = 4;

    private static final Map<String, String> NO_CTX_CHANGE = new HashMap<>(0);
    private static final Object[] NO_ARGS = new Object[0];
    private static final String TEST_CONN_METH = "testConnection";
    private static final String GET_SII_METH = "getServiceInstanceInfo";
    private static final String CXT_PROX_METH = "contextProxy";
    private static final String CLOSE_METH = "close";
    private static final String TOSTR_METH = "toString", EQUALS_METH = "equals", HC_METH = "hashCode";
    private static final int MAX_REINITS = 4;

    private static volatile Executor asyncThreads;
    private static Executor callbackExecutor;

    private Semaphore sem; //TODO this should go somewhere else (not used currently anyhow)

    private final Map<String, String> defaultContext;
    private final ServiceRegistryClient srClient;
    private final Balancers balancers;

    // avoid constructing this every time (for performance only)
    private final TException SUTException;
    private final Class<?>[] ifaces;
    private final ClassLoader loader;

    private TServiceClientManager<C> clientPool;

    private volatile boolean closed;

    private final ThriftClientBuilder tcb;

    ClientInvocationHandler(ThriftClientBuilder tcb, Map<String, String> defaultContext,
        ServiceRegistryClient srClient, TServiceClientManager<C> clientPool,
        Class<?>[] ifaces, ClassLoader loader) {
        this.tcb = tcb;
        this.defaultContext = defaultContext;
        this.srClient = srClient;
        this.clientPool = clientPool;
        this.balancers = new Balancers(tcb.lbPolicy);
        this.SUTException = LitelinksExceptions.eraseStackTrace(
            new TException(clientPool.ServiceUnavailableException));
        this.ifaces = ifaces;
        this.loader = loader;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Exception {
        return doInvoke(proxy, method, args, null);
    }

    private Object doInvoke(Object proxy, final Method method, final Object[] args,
            Map<String, String> extraContext) throws Exception {
        final String methodName = method.getName();
        final Class<?> decClass = method.getDeclaringClass();

        if (decClass == LitelinksServiceClient.class) {
            if (CLOSE_METH.equals(methodName)) {
                close();
                return null; // void
            }
            if (CXT_PROX_METH.equals(methodName)) {
                if (tcb.clientClass == null) {
                    return this;
                }
                return Proxy.newProxyInstance(loader, ifaces, new InvocationHandler() {
                    private final Map<String, String> cxt = (Map<String, String>) args[0];

                    @Override
                    public Object invoke(Object proxy, Method method, Object[] args) throws Exception {
                        return doInvoke(proxy, method, args, cxt);
                    }
                });
            }
            for (int i = 0; i < MAX_REINITS; i++) {
                try {
                    if (TEST_CONN_METH.equals(methodName)) {
                        long cmdTimeout = args != null && args.length > 0? (Long) args[0] : tcb.timeoutMillis;
                        return invokeWithRetries(null, TEST_CONN_METH, null, cmdTimeout, null);
                    }
                    if (GET_SII_METH.equals(methodName)) {
                        return clientPool.getServiceInstanceInfo();
                    }
                    // this intercepts both isAvailable() and awaitAvailable() methods
                    return /*(Boolean)*/ clientPool
                            .awaitAvailable(args != null && args.length > 0? (Long) args[0] : 0L);
                } catch (ClientClosedException cce) {
                    handleClientClosed(i >= MAX_REINITS - 1, cce);
                }
            }
        }
        if (decClass == Object.class) {
            if (TOSTR_METH.equals(methodName)) {
                return "[LLClient name=" + clientPool.getServiceName() + " iface="
                       + tcb.clientInterface + "]@" + Integer.toHexString(System.identityHashCode(proxy));
            } else if (EQUALS_METH.equals(methodName)) {
                return args != null && args.length > 0 && args[0] == proxy;
            } else if (HC_METH.equals(methodName)) {
                return System.identityHashCode(proxy);
            }
        }

        final MethodConfig mc = (MethodConfig) tcb.methodConfig.get(methodName);
        final int cmdTimeout = mc != null && mc.timeout >= 0? mc.timeout : tcb.timeoutMillis;

        if (tcb.clientAsyncInterface != null && tcb.clientAsyncInterface.isAssignableFrom(decClass)) {
            invokeAsync(methodName, args, extraContext, cmdTimeout, mc);
            return null; // void
        }

        Map<String, String> restoreTcTo = setupThreadContext(defaultContext, extraContext);
        try {
            for (int i = 0; i < MAX_REINITS; i++) {
                try {
                    return invokeWithRetries(method, methodName, args, cmdTimeout, mc);
                } catch (ClientClosedException cce) {
                    handleClientClosed(i >= MAX_REINITS - 1, cce);
                }
            }
        } finally {
            // note restoreTcTo might be null
            if (restoreTcTo != NO_CTX_CHANGE) {
                ThreadContext.revertCurrentContext(restoreTcTo);
            }
        }
        throw new IllegalStateException(); // should never reach here
    }

    private void invokeAsync(String methodName, Object[] args,
            Map<String, String> extraContext, long cmdTimeout, MethodConfig mc) {
        int len = args.length;
        final AsyncMethodCallback<Object> callback = (AsyncMethodCallback<Object>) args[len - 1];
        final Object[] syncArgs = len <= 1? NO_ARGS : Arrays.copyOf(args, len - 1);
        final Map<String, String> cxt = getThreadContextForAsync(defaultContext, extraContext);
        final Map<String, String> mdc = clientPool.sendMDC? MDC.getCopyOfContextMap() : null;
        final long deadlineTimeout = ThreadContext.nanosUntilDeadline();
        asyncExecutor().execute(() -> {
            if (mdc != null) {
                MDC.setContextMap(mdc);
            }
            ThreadContext.setCurrentContext(cxt, deadlineTimeout);
            Throwable err = null;
            Object response = null;
            try {
                try {
                    // loop is just for re-initializing closed clients (via catch block)
                    for (int i = 0; i < MAX_REINITS; i++) {
                        try {
                            response = invokeWithRetries(mc.syncMethod, methodName, syncArgs, cmdTimeout, mc);
                            break;
                        } catch (ClientClosedException cce) {
                            handleClientClosed(i >= MAX_REINITS - 1, cce);
                        }
                    }
                } catch (Throwable t) {
                    err = t;
                }
                if (callbackExecutor == null) {
                    doCallback(methodName, callback, response, err);
                } else {
                    boolean ok = false;
                    final AutoCloseable toRelease = !POOLED_BUFS? null
                            : ReleaseAfterResponse.takeOwnership();
                    try {
                        final Object finalResponse = response;
                        final Throwable finalErr = err;
                        callbackExecutor.execute(() -> {
                            try {
                                if (mdc != null) {
                                    MDC.setContextMap(mdc);
                                }
                                doCallback(methodName, callback, finalResponse, finalErr);
                            } finally {
                                closeQuietly(toRelease);
                                if (mdc != null) {
                                    MDC.clear();
                                }
                            }
                        });
                        ok = true;
                    } finally {
                        if (!ok) {
                            closeQuietly(toRelease);
                        }
                    }
                }
            } finally {
                if (POOLED_BUFS) {
                    ReleaseAfterResponse.releaseAll();
                }
                if (cxt != null) {
                    ThreadContext.removeCurrentContext();
                }
                if (mdc != null) {
                    MDC.clear();
                }
            }
            if (err instanceof Error) {
                throw (Error) err;
            }
        });
    }

    private void handleClientClosed(boolean giveUp, ClientClosedException cce) {
        if (giveUp || closed) {
            throw cce;
        }
        synchronized (this) {
            if (closed) {
                throw cce;
            }
            // if this particular client instance hasn't been closed, attempt to
            // refresh underlying pool
            if (!clientPool.isValid()) {
                logger.info("Refreshing underlying client pool "
                            + "for Thrift client, service=" + tcb.serviceName);
                TServiceClientManager<C> oldClientPool = clientPool;
                clientPool = tcb.getFreshClientPool(srClient);
                oldClientPool.close();
            }
        }
    }

    private void close() {
        if (!closed) {
            synchronized (this) {
                if (closed) {
                    return;
                }
                clientPool.close();
                if (tcb.listeners != null) {
                    clientPool.removeListeners(tcb.listeners);
                }
                closed = true;
            }
        }
    }

    @Override
    protected void finalize() throws Throwable {
        if (logger.isDebugEnabled()) {
            logger.debug("in finalize for " + this + "(serviceName="
                         + tcb.serviceName + ") with " + clientPool);
        }
        close();
    }

    // returns map to restore threadcontext to post-invocation
    private final Map<String, String> setupThreadContext(Map<String, String> defaultContext,
            Map<String, String> extraContext) {
        if (defaultContext == null && extraContext == null) {
            return NO_CTX_CHANGE;
        }
        Map<String, String> tcBefore = ThreadContext.getCurrentContext();
        ThreadContext.setCurrentContext(mergedThreadContext(tcBefore, defaultContext, extraContext));
        return tcBefore;
    }

    private final Map<String, String> getThreadContextForAsync(Map<String, String> defaultContext,
            Map<String, String> extraContext) {
        Map<String, String> currContext = ThreadContext.getCurrentContext();
        if (defaultContext == null && extraContext == null) {
            return currContext == null? currContext : ImmutableMap.copyOf(currContext);
        } else {
            return mergedThreadContext(currContext, defaultContext, extraContext);
        }
    }

    private final Map<String, String> mergedThreadContext(Map<String, String> currContext,
            Map<String, String> defaultContext, Map<String, String> extraContext) {
        Map<String, String> newTc = null;
        if (sz(currContext) == 0) {
            if (extraContext == null) {
                newTc = defaultContext;
            } else if (defaultContext == null) {
                newTc = extraContext;
            }
        }
        if (newTc == null) {
            // merge, existing TC takes precedence
            newTc = Maps.newHashMapWithExpectedSize(
                    sz(currContext) + sz(defaultContext) + sz(extraContext));
            if (defaultContext != null) {
                newTc.putAll(defaultContext);
            }
            if (currContext != null) {
                newTc.putAll(currContext);
            }
            if (extraContext != null) {
                newTc.putAll(extraContext);
            }
        }
        return newTc;
    }

    private Object invokeWithRetries(Method method, String methodName, Object[] args,
            long cmdTimeoutMs, MethodConfig mc) throws Exception {
        // null method arg means send PING
        final boolean pingOnly = method == null;

        try {
            final long deadlineNanos = getDeadlineNanos(cmdTimeoutMs);

            if (sem != null && !sem.tryAcquire(remainingTimeNanos(deadlineNanos), TimeUnit.NANOSECONDS)) {
                throw new TooManyConnectionsException(tcb.serviceName);
            }
            try {
                for (int i = 0; ; i++) {
                    FailType failure = null;
                    ServiceInstance<C>.PooledClient pc = clientPool.borrowClient(balancers, methodName, args);
                    try {
                        long thriftTimeoutMs = remainingTimeNanos(deadlineNanos) / 1000_000L;
                        if (!pingOnly) {
                            return invoke(pc.getClient(), method, args, thriftTimeoutMs);
                        } else {
                            clientPool.testConnection(pc.getClient(), thriftTimeoutMs);
                            return null;
                        }
                    } catch (Exception e) {
                        boolean interrupted = Thread.interrupted(), interruption = isInterruption(e);
                        if (interrupted && !interruption) {
                            e = (Exception) new InterruptedException().initCause(e);
                            interruption = true;
                        }
                        if (!interruption && clientPool.isTransportException(e)) {
                            failure = FailType.CONN;
                            if (!pingOnly && !isRetryable(e) && !pc.getMethodInfo(methodName).isIdempotent()) {
                                logger.info("Non-retryable method failure occurred"
                                            + " (serviceName=" + tcb.serviceName
                                            + ", method=" + methodName + ")");
                                //TODO need to consider timeout here:
                                // shortened timeout shouldn't necessarily trigger failover
                                throw e;
                            }
                            // else fall-thru to retry
                        } else {
                            if (pingOnly || e instanceof RuntimeException) {
                                failure = FailType.OTHER;
                                throw e;
                            } else if (interruption) {
                                failure = FailType.OTHER;
                                throw throwRuntimeExceptionOrTException(e);
                            } else {
                                MethodInfo mi = pc.getMethodInfo(methodName);
                                Set<Class<? extends Exception>> rets = mi.instanceFailureExceptions();
                                if (rets == null || rets.isEmpty() || !rets.contains(e.getClass())) {
                                    // app-defined exception - not considered failure
                                    throw e;
                                }
                                failure = FailType.INTERNAL;
                                if (!mi.isIdempotent()) {
                                    throw e;
                                }
                                // else fall-thru to retry
                            }
                        }
                        // failure is retryable (retriable?)
                        if (i >= MAX_RETRIES) {
                            logger.info("Maximum retries (" + MAX_RETRIES + ") reached"
                                        + " (serviceName=" + tcb.serviceName + ", method=" + methodName + ")");
                            throw e;
                        }
                        String errMsg = "Retryable failure occurred for service="
                                        + tcb.serviceName + ", method=" + methodName + "; retrying";
                        if (logger.isDebugEnabled()) {
                            logger.info(errMsg, e);
                        } else {
                            logger.info(errMsg + ": " + e.getClass() + ": " + e.getMessage());
                        }
                    } finally {
                        if (pc != null) {
                            pc.releaseClient(failure);
                        }
                    }
                }
            } finally {
                if (sem != null) {
                    sem.release();
                }
            }
        } catch (Exception fee) {
            if (pingOnly) {
                throw fee;
            }
            if (isInterruption(fee)) {
                throw throwRuntimeExceptionOrTException(fee);
            }
            return invokeFallback(method, args, mc, fee);
        }
    }

    //TODO move to ServiceClientManager interface *maybe*
    private boolean isRetryable(Exception e) {
        if (e instanceof WTTransportException
            && ((WTTransportException) e).isBeforeWriting()) {
            return true;
        }
        if (!(e instanceof TApplicationException)) {
            return false;
        }
        int type = ((TApplicationException) e).getType();
        return type == TApplicationException.PROTOCOL_ERROR
               || type == TApplicationException.INVALID_PROTOCOL
               || type == TApplicationException.UNSUPPORTED_CLIENT_TYPE;
    }

    private long remainingTimeNanos(long deadlineNanos) throws TimeoutException {
        if (deadlineNanos == 0L) {
            return 0L;
        }
        long remain = deadlineNanos - nanoTime();
        if (remain <= 0L) {
            throw new TimeoutException();
        }
        return remain;
    }

    private Object invoke(TServiceClient client, Method method, Object[] args, long timeout) throws TException {
        try {
            TTransport tt = client.getOutputProtocol().getTransport();
            if (tt instanceof NettyTTransport) {
                ((NettyTTransport) tt).startIOTimer(timeout);
            } else if (tt instanceof TSocket) {
                ((TSocket) tt).setTimeout((int) timeout);
            }

            return method.invoke(client, args);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw throwTExceptionOrRuntimeException(e.getCause());
        }
    }

    private Object invokeFallback(Method method, final Object[] args, final MethodConfig mc,
            final Exception fee) throws TException {
        final Cause reason = mapExceptionToCause(fee);
        if (logger.isDebugEnabled()) {
            final String msg = "Invocation failure for service "
                               + tcb.serviceName + ", method=" + method.getName() + ", cause=" + reason;
            if (fee == null) {
                logger.debug(msg); // log level TBD
            } else {
                logger.debug(msg, fee);
            }
        }

        if (mc != null && mc.fallback != null) {
            return mc.fallback.getFallback(new FailureInformation() {
                @Override public Object[] getOriginalArgs() {
                    return args;
                };
                @Override public Throwable getException() {
                    return fee;
                }
                @Override public Cause getCause() {
                    return reason != null? reason : mapExceptionToCause(fee);
                }
            });
        }
        if (tcb.fallbackClass != null) {
            try {
                return method.invoke(tcb.fallbackClass, args);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            } catch (InvocationTargetException e) {
                throw throwTExceptionOrRuntimeException(e.getCause());
            }
        }
        if (fee == clientPool.ServiceUnavailableException) { //TODO or origclientpool
            throw SUTException;
        }
        throw throwRuntimeExceptionOrTException(fee);
    }

    /**
     * @return 0 for no timeout, or deadline relative to System.nanoTime()
     */
    private static long getDeadlineNanos(long timeoutMs) throws TimeoutException {
        // override provided timeout with threadcontext deadline if present
        long cxtTimeoutNanos = ThreadContext.nanosUntilDeadline(), timeoutNanos;
        if (timeoutMs == 0L) {
            if (cxtTimeoutNanos == -1L) {
                return 0L;
            }
            timeoutNanos = cxtTimeoutNanos;
        } else {
            timeoutNanos = timeoutMs * 1000_000L;
            if (cxtTimeoutNanos >= 0L && cxtTimeoutNanos < timeoutNanos) {
                timeoutNanos = cxtTimeoutNanos;
            }
        }
        if (timeoutNanos < 3L) {
            throw new TimeoutException();
        }
        long deadline = nanoTime() + timeoutNanos;
        return deadline == 0L? 1L : deadline;
    }

    private static int sz(Map<?, ?> c) {
        return c == null? 0 : c.size();
    }

    static Executor asyncExecutor() {
        Executor exec = asyncThreads;
        if (exec == null) {
            synchronized (ThriftClientBuilder.class) {
                if ((exec = asyncThreads) != null) {
                    return exec;
                }
                boolean serial = Boolean.getBoolean(LitelinksSystemPropNames.SERIAL_CALLBACKS);
                if (serial) {
                    callbackExecutor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
                            .setNameFormat("ll-async-callback-thread").setDaemon(true).build());
                    logger.info("Async callbacks will be made from single thread ll-async-callback-thread");
                }
                //TODO make this size configurable
                asyncThreads = exec = Executors.newCachedThreadPool(new ThreadFactoryBuilder()
                        .setNameFormat("ll-async-client-thread-%d")
                        .setDaemon(true).setThreadFactory(FastThreadLocalThread::new).build());
            }
        }
        return exec;
    }


    static RuntimeException throwTExceptionOrRuntimeException(Throwable t) throws TException {
        if (t instanceof TException) {
            throw (TException) t;
        }
        throw t instanceof RuntimeException? (RuntimeException) t : new RuntimeException(t);
    }

    static TException throwRuntimeExceptionOrTException(Throwable t) throws TException {
        if (t instanceof RuntimeException) {
            throw (RuntimeException) t;
        }
        throw t instanceof TException? (TException) t : new TException(t);
    }

    static void closeQuietly(AutoCloseable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception e) {
                logger.warn("Error closing/releasing resources", e);
            }
        }
    }

    static <T> void doCallback(String methodName, AsyncMethodCallback<T> callback,
            T response, Throwable err) {
        try {
            if (err == null) {
                callback.onComplete(response);
            } else {
                callback.onError(err instanceof Exception
                        ? (Exception) err : new RuntimeException(err));
            }
        } catch (RuntimeException rte) {
            logger.error("Async callback for method " + methodName + " threw exception", rte);
        }
    }

    private static Cause mapExceptionToCause(Throwable t) {
        if (t instanceof TException) {
            if (t instanceof TTransportException)
                return ((TTransportException) t).getType() == TTransportException.TIMED_OUT?
                        Cause.TIMEOUT : Cause.CONN_FAILURE;
            if (t instanceof TProtocolException) return Cause.CONN_FAILURE;
            return Cause.APP_EXCEPTION; // includes TApplicationException
        }
        if (t instanceof ServiceUnavailableException) return Cause.UNAVAILABLE;
        if (t instanceof TimeoutException) return Cause.TIMEOUT;
        if (t instanceof SocketException) return Cause.CONN_FAILURE;
        if (t instanceof TooManyConnectionsException) return Cause.TOO_MANY_CONNS;
        return t != null? Cause.LOCAL_FAILURE : Cause.UNKNOWN; //TBD
    }
}
