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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.ibm.watson.litelinks.ThreadPoolHelper;
import com.ibm.watson.litelinks.ThriftConnProp;
import com.ibm.watson.litelinks.server.ZookeeperWatchedService;
import com.ibm.watson.zk.ZookeeperClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.ibm.watson.litelinks.server.ZookeeperWatchedService.getServicePath;

/**
 * Zookeeper-based service discovery client implementation
 */
public class ZookeeperServiceRegistry implements ServiceRegistryClient {

    public final String connString;

    public ZookeeperServiceRegistry(String connString) {
//		if(connString == null) throw new IllegalArgumentException();
        this.connString = ZookeeperClient.resolveConnString(connString);
    }

    @Override
    public ServiceWatcher newServiceWatcher(String serviceName) {
        return new ZookeeperServiceWatcher(serviceName);
    }

    private static final Logger logger = LoggerFactory.getLogger(ZookeeperServiceWatcher.class);

    public static final int CONF_UNAVAILABLE_TIMEOUT_SECS = 3;
    private static final int MAX_THREADS = 32;

    private static final ThreadPoolExecutor sharedThreadPool =
            ThreadPoolHelper.newThreadPool(1, MAX_THREADS, 3, TimeUnit.HOURS,
                    ThreadPoolHelper.threadFactory("ll-zk-discovery-thread-%d"));

    protected static final Listener.Server[] NO_SERVERS = new Listener.Server[0];

    /**
     *
     */
    public final class ZookeeperServiceWatcher extends ServiceWatcher implements PathChildrenCacheListener {

        /* Soon, the NodeCache & PathChildrenCache will likely be replaced by a single TreeCache
         */

        private final String serviceName, zkPath;
        private final CuratorFramework curator; // curator wraps zookeeper client
        private final NodeCache nodeCache; //this is the node cache for this service
        private final PathChildrenCache childCache; //this is the child (instance) cache for this service
        private final SerializingExecutorService cacheExecutor; // serialized executor used by the caches
        private boolean childCacheStarted; // non-volatile optimization
        private volatile boolean closed;

        private final SettableFuture<Void> initFuture = SettableFuture.create();

        private volatile Map<Object, Object> currentConfig;
        private long currentConfigMxid;

        public ZookeeperServiceWatcher(String serviceName) {
            super(serviceName);
            if (serviceName.indexOf('/') >= 0) {
                throw new IllegalArgumentException("service name must not contain '/'");
            }

            this.serviceName = serviceName;
			this.zkPath = getServicePath(serviceName);
            //TODO TBD should below be done here or in start()
            this.curator = ZookeeperClient.getCurator(connString);
            if (curator == null) {
                throw new IllegalArgumentException("no zookeeper conn string specified");
            }
            this.cacheExecutor = new SerializingExecutorService(sharedThreadPool) {
                @Override
                protected void logTaskUncheckedException(Throwable t) {
                    logger.error("Exception from ZK Discovery task for service "
                                 + ZookeeperServiceWatcher.this.serviceName, t);
                }
            };
			this.nodeCache = new NodeCache(curator, zkPath);
			this.childCache = new PathChildrenCache(curator, zkPath, true, false, cacheExecutor);
        }

        @Override
        protected ServiceRegistryClient getRegistryClient() {
            return ZookeeperServiceRegistry.this;
        }

        @Override
        protected ListenableFuture<Void> startAsync() {
            try {
                nodeCache.getListenable().addListener(() -> {
                    synchronized (ZookeeperServiceRegistry.this) {
                        ChildData cdata = nodeCache.getCurrentData();
                        if (logger.isDebugEnabled()) {
                            logger.debug("service " + serviceName + " node change event: "
                                         + (cdata == null || cdata.getStat() == null? "null"
                                    : "new zk vers=" + cdata.getStat().getVersion()));
                        }
                        processDataChange(cdata);
                    }
                }, cacheExecutor);
                childCache.getListenable().addListener(this);

                boolean nodeExists = false;
                synchronized (this) {
                    nodeCache.start(true); //TODO this can hang if ZK not available
                    ChildData cdata = nodeCache.getCurrentData();
                    if (cdata != null) {
                        nodeExists = true;
                        processDataChange(cdata);
                        //TODO case where ZK down from start TBD
                    }
                }
                if (nodeExists) {
                    return initFuture;
                } else {
                    logger.info("Service " + serviceName + " doesn't yet exist; childCache start delayed");
                    return Futures.immediateFuture(null);
                }
            } catch (Exception e) {
                close();
                return Futures.immediateFailedFuture(e);
            }
        }

        // always called from synchronized context
        private void processDataChange(ChildData cdata) throws Exception {
            if (cdata == null) {
                currentConfig = null;
                currentConfigMxid = 0;
            } else {
                boolean nodeCreation = currentConfig == null;
                Stat newStat = cdata.getStat();
                boolean dataChanged = nodeCreation || newStat.getMzxid() != currentConfigMxid;
                if (dataChanged) {
                    byte[] svcData = cdata.getData();
                    Properties props = new Properties();
                    if (svcData != null && svcData.length > 0) {
                        props.load(new ByteArrayInputStream(svcData)); // ISO8859-1
                    }
                    currentConfig = props;
                    currentConfigMxid = newStat.getMzxid();
                    if (logger.isDebugEnabled()) {
                        logger.debug("New client config from ZK for service " + serviceName + ":");
                        ThriftConnProp.log(logger, props);
                    }
                }
                if (!childCacheStarted) {
                    try {
                        // assert nodeCreation;
                        childCache.start(StartMode.POST_INITIALIZED_EVENT);
                        childCacheStarted = true;
                        logger.info("started the childCache for service " + serviceName);
                        return;
                    } catch (IllegalStateException ise) { // this is ok, means already started
                        childCacheStarted = true;
                    }
                }
                // already started
                if (nodeCreation || dataChanged && newStat.getNumChildren() > 0) {
                    childCache.clearAndRefresh();
                    logger.info("refreshed the childCache for service " + serviceName);
                }
            }
        }

        @Override
        public void close() {
            synchronized (this) {
                closed = true;
                try {
                    nodeCache.close();
                } catch (IOException e) {
                } // ignore errors
                try {
                    childCache.close();
                } catch (IOException e) {
                } // ignore errors
            }
            cacheExecutor.shutdown();
            try {
                cacheExecutor.awaitTermination(500L, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public boolean isValid() {
            return !closed && curator.getState() != CuratorFrameworkState.STOPPED;
            //TODO nodeCache/childCache don't expose their state - should also check that here
        }

        @Override
        protected boolean isAvailable() {
            List<ChildData> cds = childCache.getCurrentData();
            return cds != null && !cds.isEmpty();
        }

        @Override
        protected boolean doConfirmUnavailable() {
            try {
                return runTimeboxed(sharedThreadPool, CONF_UNAVAILABLE_TIMEOUT_SECS, () -> {
                    // bit convoluted - need to workaround curator abstractions
                    curator.getZookeeperClient().getZooKeeper().sync(zkPath, null, null);
                    nodeCache.rebuild();
                    ChildData cdata;
                    synchronized (ZookeeperServiceRegistry.this) {
                        processDataChange(cdata = nodeCache.getCurrentData());
                    }
                    if (cdata == null || cdata.getStat().getNumChildren() == 0) {
                        return Boolean.TRUE;
                    }
                    logger.info("Service " + serviceName + " found available after sync zk verification");
                    initFuture.get(3, TimeUnit.SECONDS);
                    Future<Void> fut = cacheExecutor.submit(() -> {
                        childCache.rebuild();
                        refresh(childCache.getCurrentData());
                        return null;
                    });
                    // child events will now be in executor queue
                    cacheExecutor.waitUntilIdle();
                    // child events will now have all completed
                    fut.get(); // throw exception if it failed
                    return Boolean.FALSE;
                });
            } catch (TimeoutException e) {
                logger.warn("Timeout confirming unavailability of service: " + serviceName, e);
            } catch (Exception e) {
                Throwable ex = e;
                if (ex instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                } else if (ex instanceof ExecutionException) {
                    ex = e.getCause();
                    if (ex instanceof KeeperException.NoNodeException) {
                        return true; // ok, confirmed unavailable
                    }
                    if (ex instanceof Error) {
                        throw (Error) ex;
                    }
                }
                logger.warn("Exception confirming unavailability of service: " + serviceName, ex);
            }
            return true;
        }

        @Override
        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) {
            if (closed) {
                return; // ignore
            }
            try {
                if (logger.isDebugEnabled()) {
                    logger.debug("Zookeeper event: " + event.getType() + " path=" +
                                 (event.getData() != null? event.getData().getPath() : "null"));
                }
                switch (event.getType()) {
                case INITIALIZED:
                    refresh(event.getInitialData());
                    initFuture.set(null);
                    break;
                case CHILD_UPDATED: // the update event should not happen
                    logger.info("Existing service instance data changed (unexpected)");
                    // (fall-thru)
                case CHILD_ADDED:
                    // ignore pre-initialize adds
                    // (they are added in one go in the INITIALIZED event)
                    if (initFuture.isDone()) {
                        addChild(event.getData());
                    }
                    break;
                case CHILD_REMOVED:
                    getListener().serverRemoved(event.getData().getPath());
                    break;
                case CONNECTION_LOST:
                case CONNECTION_RECONNECTED:
                case CONNECTION_SUSPENDED:
                default:
                    break;
                }
            } catch (RuntimeException rte) {
                logger.error("Exception while processing zookeeper event", rte);
            }
        }

        private void refresh(List<ChildData> cds) {
            if (cds == null) {
                return;
            }
            if (cds.isEmpty()) {
                getListener().refreshServerList(NO_SERVERS);
            } else {
                final List<Listener.Server> servers = new ArrayList<>(cds.size());
                for (int i = 0; i < cds.size(); i++) {
                    final Listener.Server svr = getServer(cds.get(i));
                    if (svr != null) {
                        servers.add(svr);
                    }
                }
                getListener().refreshServerList(servers.toArray(new Listener.Server[servers.size()]));
            }
        }

        protected void addChild(ChildData cd) {
            final Listener.Server svr = getServer(cd);
            if (svr != null) {
                getListener().serverAdded(svr.hostname, svr.port,
                        svr.registrationTime, svr.version, svr.key, svr.instanceId, svr.connConfig);
            }
        }

        private Listener.Server getServer(ChildData cd) {
            final String key = cd.getPath();
            byte[] nodeData = cd.getData();
            if (nodeData != null) {
                String nodeStr = new String(nodeData, StandardCharsets.ISO_8859_1);
                String[] parts = nodeStr.split(":");
                Map<Object, Object> config = currentConfig;
                if (parts.length >= 2) {
                    String version = null, instanceId = null;
                    int br = nodeStr.indexOf('\n');
                    if (nodeStr.length() > br + 1) {
                        Properties props = new Properties();
                        if (config != null) {
                            props.putAll(config);
                        }
                        try {
                            props.load(new StringReader(nodeStr.substring(br + 1)));
                            version = (String) props.remove(ZookeeperWatchedService.SERVICE_VERSION);
                            instanceId = (String) props.remove(ZookeeperWatchedService.INSTANCE_ID);
                            config = props;
                        } catch (IOException e) {
                            logger.warn("Error parsing config for server "
                                        + key + " (" + parts[0] + ":" + parts[1] + ")", e);
                        }
                    }
                    if (instanceId == null) {
                        // use last component of znode path if server doesn't provide id
                        int ls = key.lastIndexOf('/');
                        instanceId = ls >= 0? key.substring(ls + 1) : key;
                    }
                    return new Listener.Server(parts[0], Integer.parseInt(parts[1]),
                            cd.getStat().getCtime(), version, key, instanceId, config);
                } else {
                    logger.warn("ignoring new child " + key + " with unrecognized data '"
                                + parts[0] + "' for service " + serviceName);
                }
            } else {
                logger.warn("ignoring new child " + key + " with null data for service " + serviceName);
            }
            return null;
        }

        @Override
        protected void finalize() throws Throwable {
            close();
        }
    }

    @Override
    public int hashCode() {
        return 31 + (connString == null? 0 : connString.hashCode());
    }

    @Override
    public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null || getClass() != obj.getClass()) return false;
        ZookeeperServiceRegistry other = (ZookeeperServiceRegistry) obj;
        return Objects.equals(connString, other.connString);
    }

    public static ZookeeperServiceRegistry getDefault() {
        final String defaultConnString = ZookeeperClient.resolveConnString(null); // will come from env var
        return defaultConnString != null? new ZookeeperServiceRegistry(defaultConnString) : null;
    }

    // utility method
    public static <T> T runTimeboxed(ExecutorService exec, int timoutSecs, final Callable<T> command)
            throws InterruptedException, TimeoutException, ExecutionException {
        final Thread[] t = new Thread[1];
        Callable<T> task = () -> {
            t[0] = Thread.currentThread();
            try {
                return command.call();
            } finally {
                t[0] = null;
            }
        };
        Future<T> fut = exec.submit(task);
        try {
            return fut.get(timoutSecs, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            fut.cancel(true);
            throw e;
        } catch (TimeoutException te) {
            Thread ts = t[0];
            if (ts != null) {
                te = new TimeoutException("Stacktrace at timeout:");
                te.setStackTrace(ts.getStackTrace());
            }
            fut.cancel(true);
            throw te;
        }
    }


    // instrumentation only
    public static int getMaxThreadUsage() {
        return sharedThreadPool.getLargestPoolSize();
    }


}
