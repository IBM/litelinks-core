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

import com.google.common.base.Preconditions;
import com.ibm.watson.litelinks.LitelinksSystemPropNames;
import com.ibm.watson.litelinks.MethodInfo;
import com.ibm.watson.litelinks.ThreadPoolHelper;
import com.ibm.watson.litelinks.client.ServiceInstanceCache.ServiceClientManager;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectState;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.Collections;
import java.util.Deque;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Instances of this class correspond to a single remote service instance.
 * It contains the pool of TServiceClients (connections).
 *
 * @param <C> client class
 */
@SuppressWarnings("serial")
public final class ServiceInstance<C> extends AtomicReference<ServiceInstance.ServiceInstanceConfig<C>>
        implements LitelinksServiceClient.ServiceInstanceInfo {

    private static final Logger logger = LoggerFactory.getLogger(ServiceInstance.class);

    abstract static class ServiceInstanceConfig<C> {
        private final String version;
        private final long registrationTime;
        private final Map<String, String> metadata;
        private final Map<String, MethodInfo> methodInfos;
        private final MethodInfo defaultMethodInfo;

        public ServiceInstanceConfig(String version, long registrationTime,
                Map<String, String> metadata, Map<String, MethodInfo> methodInfos) {
            this.version = version;
            this.registrationTime = registrationTime;
            this.metadata = metadata;
            this.methodInfos = methodInfos != null ? methodInfos : Collections.emptyMap();
            MethodInfo defaultInfo = methodInfos.get(MethodInfo.DEFAULT);
            defaultMethodInfo = defaultInfo != null ? defaultInfo : MethodInfo.DEFAULT_MI;
        }

        abstract String getHost();

        abstract int getPort();

        // service-specific, may be null
        String getVersion() {
            return version;
        }

        long getRegistrationTime() {
            return registrationTime;
        }

        Map<String, String> getMetadata() {
            return metadata;
        }

        // will not return null
        Map<String, MethodInfo> getMethodInfos() {
            return methodInfos;
        }

        // will not return null
        MethodInfo getDefaultMethodInfo() {
            return defaultMethodInfo;
        }

        @Override
        public String toString() {
            String host = getHost(), vers = getVersion();
            int port = getPort();
            StringBuilder sb = new StringBuilder(host != null ? host : "null")
                    .append(':').append(port);
            return (vers == null ? sb : sb.append(";v=").append(vers)).toString();
        }
    }

    private static final long MAX_CONN_RETRY_INTERVAL_MILLIS = 30000L; // 30sec

    private static final ScheduledExecutorService connRetryThreadPool
            = Executors.newScheduledThreadPool(2, //TODO consider larger pool
            ThreadPoolHelper.threadFactory("ll-conn-retry-thread-%d"));

    private final String instanceId;

    final ServiceClientManager<C> clientMgr;
    final ServiceInstanceCache<C> owner;

    final ServiceRegistryClient source;

    public enum State {
        ACTIVE, FAILING, INACTIVE
    }

    // state is managed from the containing ServiceInstanceCache class
    private final AtomicReference<State> state = new AtomicReference<>(State.ACTIVE);
    private State lazyState = state.get();

    private volatile Runnable lastRetryTask;

    // it's more efficient to keep track of the count separately than
    // continually querying the size of the pool
    private final AtomicInteger inUseCount = new AtomicInteger();
    private long lastUsed; // non-volatile for perf - precision not needed

    private final ObjectPool<PooledClient> connPool;

    State setState(State newState) {
        lazyState = newState;
        return state.getAndSet(newState);
    }

    boolean changeState(State from, State to) {
        boolean changed = state.compareAndSet(from, to);
        if (changed) {
            lazyState = to;
        }
        return changed;
    }

    private static final GenericObjectPoolConfig<?> poolConfig;

    static {
        poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setMaxIdle(Runtime.getRuntime().availableProcessors());
        poolConfig.setMaxTotal(-1); //unlimited
        poolConfig.setTestOnBorrow(true);
        // no objects will be evicted due to idle time, but rather if validation fails
        // (conn is known to be closed)
        poolConfig.setTestWhileIdle(true);
        poolConfig.setTimeBetweenEvictionRunsMillis(317000); // 5min, 17sec
    }

    public int getInUseCount() {
        return inUseCount != null ? inUseCount.get() : connPool.getNumActive();
    }

    public long getLastUsedTime() {
        return lastUsed;
    }

    @SuppressWarnings("unchecked")
    public ServiceInstance(String id, ServiceInstanceConfig<C> config,
            ServiceInstanceCache<C> owner, ServiceRegistryClient sourceRegistry) {
        super(config);
        Preconditions.checkArgument(config != null || owner == null);
        this.instanceId = id;
        this.owner = owner;
        this.clientMgr = owner != null ? owner.getClientManager() : null;
        this.source = sourceRegistry;
        this.connPool = owner == null ? null : new GenericObjectPool<>(
                new BasePooledObjectFactory<PooledClient>() {
                    @Override
                    public PooledClient create() throws Exception {
                        final ServiceInstanceConfig<C> sic = get();
                        try {
                            if (sic == null) {
                                throw new IllegalStateException("missing client config");
                            }
                            return new PooledClient(clientMgr.createClient(sic,
                                    //TODO timeout tbd -- maybe threadlocal
                                    ServiceInstanceCache.NEWCLIENT_CONN_TIMEOUT));
                        } catch (Throwable t) {
                            // Include stacktrace if debug is enabled
                            if (logger.isDebugEnabled()) {
                                logger.error("Failed to open new connection to " + sic, t);
                            } else {
                                logger.error("Failed to open new connection to " + sic + ": " + t);
                            }
                            if (t instanceof IllegalStateException) {
                                // It's important that we don't throw ISE from here since that is also used
                                // to indicate a closed object pool.
                                throw new RuntimeException(t);
                            }
                            throw t;
                        }
                    }

                    @Override
                    public PooledObject<PooledClient> wrap(PooledClient client) {
                        // can revert to this if extra pooling stats are needed
                        //return new DefaultPooledObject<PooledClient>(client);
                        return client;
                    }

                    @Override
                    public void destroyObject(PooledObject<PooledClient> po) {
                        C client = po.getObject().client;
                        if (logger.isDebugEnabled()) {
                            logger.debug("about to destroy: " + client);
                        }
                        clientMgr.close(client);
                    }

                    @Override
                    public boolean validateObject(PooledObject<PooledClient> po) {
                        // this will be invoked prior to borrowing, and also periodically
                        // on idle client objects in the pool
                        boolean valid;
                        try {
                            valid = clientMgr.isValid(po.getObject().getClient());
                        } catch (NullPointerException npe) {
                            logger.warn("Invalid pooled client (null)", npe);
                            valid = false;
                        }
                        //TODO here should trigger conn/svc instance failover
                        return valid;
                    }

                }, (GenericObjectPoolConfig<PooledClient>) poolConfig);
    }

    PooledClient borrowClient() throws Exception {
        if (state.get() == State.INACTIVE) {
            throw new IllegalStateException();
        }
        try {
            PooledClient pc = connPool.borrowObject(); // throws ISE if closed
            if (inUseCount != null) {
                inUseCount.incrementAndGet();
            }
            return pc;
        } catch (Exception e) {
            if (clientMgr.isTransportException(e)) {
                failureOccurred(true, true);
            }
            throw e;
        }
    }

    enum FailType {CONN, INTERNAL, OTHER}

    void returnClient(PooledClient client, FailType failType) throws Exception {
//      System.out.println("returning client w trans "+client.getClient().getInputProtocol().getTransport()+" suc="+success);
        if (inUseCount != null) {
            lastUsed = System.currentTimeMillis();
            inUseCount.decrementAndGet();
        }
        if (failType == FailType.CONN) {
            failureOccurred(true, false);
        } else if (failType == FailType.INTERNAL) {
            failureOccurred(false, false);
        } else if (failType == null && client.sourceConfig == get()) {
            // identity equality intentional here
            connPool.returnObject(client);
            connSuccess();
            return;
        }
        connPool.invalidateObject(client);
    }

    void updateConfig(ServiceInstanceConfig<C> config) {
        Preconditions.checkNotNull(config, "config");
        for (; ; ) {
            final ServiceInstanceConfig<C> thiscfg = get();
            if (Objects.equals(thiscfg, config)) {
                return; // no change
            }
            if (compareAndSet(thiscfg, config)) {
                break; // else loop
            }
        }
        clearConnPool(); //TODO this should probably be done in a separate thread
    }

    private void clearConnPool() {
        try {
            connPool.clear();
        } catch (Exception e) {
        } // GOP doesn't throw
    }

    // visibility TBD
    void close() {
        // this will quiesce checked-out clients (connections), closing them upon return
        if (logger.isDebugEnabled()) {
            logger.debug("SI.close called; # checkedout: " + connPool.getNumActive());
        }
        lastRetryTask = null;
        connPool.close();
    }

    private void failureOccurred(boolean connFailure, boolean onCreate) {
        if (!owner.notifyFailed(this, true)) {
            return;
        }
        // if failure was connection-related, start a background task to retry the conn
        if (connFailure) {
            startTesterTask(onCreate);
        }
        // otherwise just schedule reactivation after fixed delay.
        // currently this only applies a method threw an exception defined
        // as being indicative of "instance failure" (FailType.INTERNAL)
        else {
            // default deactivate duration is 90 seconds
            final long deactivateDuration = Integer.getInteger(LitelinksSystemPropNames.INSTANCE_FAIL_DELAY_SECS, 90);
            logger.warn("Deactivating " + this + " for " + deactivateDuration + "s after failure");
            connRetryThreadPool.schedule(lastRetryTask = new Runnable() {
                @Override
                public void run() {
                    if (lastRetryTask != this || state.get() != State.FAILING) {
                        return;
                    }
                    // reactivate
                    if (owner.notifyFailed(ServiceInstance.this, false)) {
                        logger.info("Reactivating "+ServiceInstance.this+" after disabling for "+deactivateDuration+"s");
                    }
                }
            }, deactivateDuration, TimeUnit.SECONDS);
        }
    }

    private void connSuccess() {
        if (lazyState == State.FAILING) {
            owner.notifyFailed(this, false);
        }
    }

    void startTesterTask(final boolean onCreate) {
        connRetryThreadPool.submit(lastRetryTask = new Runnable() {
            final ObjectPool<PooledClient> connPool = ServiceInstance.this.connPool;
            final boolean debug = logger.isDebugEnabled();
            boolean first = true;
            long nextDelay = onCreate ? 3000L : 40L;

            @Override
            public void run() {
                if (lastRetryTask != this || state.get() != State.FAILING) {
                    return;
                }
                if (first) {
                    first = false;
                    clearConnPool(); //TODO TBC - this will clear all of the other idle connections
                } else {
                    try {
                        // this will attempt to make a new connection
                        if (debug) {
                            logger.debug("About to attempt new connection attempt."
                                    + " service=" + owner.getServiceName() + " instance=" +
                                    ServiceInstance.this);
                        }
                        boolean ok = false;
                        PooledClient pc = connPool.borrowObject();
                        try {
                            clientMgr.testConnection(pc.getClient(), 3000L);
                            ok = true;
                        } finally {
                            if (ok) {
                                connPool.returnObject(pc);
                            } else {
                                connPool.invalidateObject(pc);
                            }
                        }
                        logger.info("Connection reinstated to service " + owner.getServiceName() + " instance " +
                                ServiceInstance.this);
                        // success, reactivate
                        owner.notifyFailed(ServiceInstance.this, false);
                        return; // service now active
                    } catch (IllegalStateException e) {
                        return; // service now inactive
                    } catch (Throwable t) {
                        if (!clientMgr.isTransportException(t)) {
                            logger.error("Unexpected exception in conn retry task for service "
                                    + owner.getServiceName(), t);
                        }
                        if (nextDelay < 2000L) {
                            nextDelay = 2000L;
                        }
                    }
                }
                // still in failing state; continue retries
                long thisDelay = nextDelay;
                // increase delay exponentially up to max
                if (thisDelay < MAX_CONN_RETRY_INTERVAL_MILLIS) {
                    nextDelay = Math.min(MAX_CONN_RETRY_INTERVAL_MILLIS, thisDelay * 3L / 2L);
                }
                // randomize to avoid thundering herd
                thisDelay = ThreadLocalRandom.current().nextLong(thisDelay, thisDelay + thisDelay / 10L);
                if (debug) {
                    logger.debug("Waiting for " + thisDelay + " ms after conn failure before retrying."
                            + " service=" + owner.getServiceName() + " instance=" + ServiceInstance.this);
                }
                connRetryThreadPool.schedule(this, thisDelay, TimeUnit.MILLISECONDS);
            }
        });
    }

    @Override
    public String toString() { // only for logging/debug
        final ServiceInstanceConfig<C> sic = get();
        return (sic != null ? sic.toString() : "?")
                + (instanceId != null ? " id=" + instanceId : "")
                + " state=" + state; //TODO TBD whether to include service name: owner.getServiceName()
    }

    @Override
    public String getHost() {
        return get().getHost();
    }

    @Override
    public int getPort() {
        return get().getPort();
    }

    @Override
    public String getVersion() {
        return get().getVersion();
    }

    @Override
    public String getInstanceId() {
        return instanceId;
    }

    @Override
    public boolean isActive() {
        return state.get() == State.ACTIVE;
    }

    @Override
    public long getRegistrationTime() {
        return get().getRegistrationTime();
    }

    @Override
    public Map<String, String> getMetadata() {
        return get().getMetadata();
    }

    @Override
    public void testConnection(long timeoutMillis) throws Exception {
        PooledClient pc = borrowClient();
        FailType failure = FailType.OTHER;
        try {
            clientMgr.testConnection(pc.getClient(), timeoutMillis);
            failure = null;
        } catch (Exception e) {
            if (clientMgr.isTransportException(e)) {
                failure = FailType.CONN;
            }
            throw e;
        } finally {
            pc.releaseClient(failure);
        }
    }

    // --------------------

    // lightweight version of PooledObject and avoids additional allocation
    class PooledClient extends AtomicReference<PooledObjectState>
            implements PooledObject<PooledClient> {

        private final C client;
        private final ServiceInstanceConfig<C> sourceConfig;
        public PooledClient(C client) {
            super(PooledObjectState.IDLE);
            this.client = Preconditions.checkNotNull(client, "client");
            sourceConfig = ServiceInstance.this.get();
        }
        public C getClient() {
            return client;
        }
        public void releaseClient(FailType failure) throws Exception {
            returnClient(this, failure);
        }
        public MethodInfo getMethodInfo(String methName) {
            MethodInfo mi = sourceConfig.getMethodInfos().get(methName);
            return mi != null ? mi : sourceConfig.getDefaultMethodInfo();
        }

        @Override
        public ServiceInstance<C>.PooledClient getObject() {
            return this;
        }

        @Override
        public boolean startEvictionTest() {
            return compareAndSet(PooledObjectState.IDLE, PooledObjectState.EVICTION);
        }
        @Override
        public boolean endEvictionTest(Deque<PooledObject<ServiceInstance<C>.PooledClient>> idleQueue) {
            if (compareAndSet(PooledObjectState.EVICTION, PooledObjectState.IDLE)) {
                return true;
            }
            if (compareAndSet(PooledObjectState.EVICTION_RETURN_TO_HEAD, PooledObjectState.IDLE)) {
                idleQueue.offerFirst(this);
            }
            return false;
        }
        @Override
        public boolean allocate() {
            if (compareAndSet(PooledObjectState.IDLE, PooledObjectState.ALLOCATED)) {
                return true;
            }
            compareAndSet(PooledObjectState.EVICTION, PooledObjectState.EVICTION_RETURN_TO_HEAD);
            return false;
        }
        @Override
        public boolean deallocate() {
            return compareAndSet(PooledObjectState.ALLOCATED, PooledObjectState.IDLE)
                    || compareAndSet(PooledObjectState.RETURNING, PooledObjectState.IDLE);
        }
        @Override
        public void invalidate() {
            set(PooledObjectState.INVALID);
        }
        @Override
        public PooledObjectState getState() {
            return get();
        }
        @Override
        public void markAbandoned() {
            set(PooledObjectState.ABANDONED);
        }
        @Override
        public void markReturning() {
            set(PooledObjectState.RETURNING);
        }

        // no-ops
        @Override
        public int compareTo(PooledObject<ServiceInstance<C>.PooledClient> other) {
            return 0;
        }
        @Override public long getCreateTime() { return 0; }
        @Override public long getActiveTimeMillis() { return 0; }
        @Override public long getIdleTimeMillis() { return 0; }
        @Override public long getLastBorrowTime() { return 0; }
        @Override public long getLastReturnTime() { return 0; }
        @Override public long getLastUsedTime() { return 0; }
        @Override public void setLogAbandoned(boolean logAbandoned) {}
        @Override public void use() {}
        @Override public void printStackTrace(PrintWriter writer) {}
    }

}
