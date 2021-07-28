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
package com.ibm.watson.litelinks.etcd;

import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.ibm.etcd.api.KeyValue;
import com.ibm.etcd.client.EtcdClient;
import com.ibm.etcd.client.FutureListener;
import com.ibm.etcd.client.config.EtcdClusterConfig;
import com.ibm.etcd.client.utils.RangeCache;
import com.ibm.watson.litelinks.client.ServiceRegistryClient;
import com.ibm.watson.litelinks.client.ServiceRegistryClient.Listener.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;


/**
 * etcd-based service discovery client implementation
 */
public class EtcdServiceRegistry implements ServiceRegistryClient {

    public final EtcdClusterConfig etcdConfig;

    protected final ByteString litelinksPrefix;

    public EtcdServiceRegistry(String etcdConfigFile) throws IOException {
        this(EtcdClusterConfig.fromJsonFileOrSimple(etcdConfigFile));
    }

    public EtcdServiceRegistry(EtcdClusterConfig etcdConfig) {
        if (etcdConfig == null) {
            throw new IllegalArgumentException();
        }
        this.etcdConfig = etcdConfig;
        litelinksPrefix = EtcdDiscovery.litelinksPrefix(etcdConfig.getRootPrefix());
    }

    @Override
    public ServiceWatcher newServiceWatcher(String serviceName) throws Exception {
        return new EtcdServiceWatcher(serviceName);
    }

    private static final Logger logger = LoggerFactory.getLogger(EtcdServiceWatcher.class);

    public static final int CONF_UNAVAILABLE_TIMEOUT_MS = 3_000;

    /**
     *
     */
    public final class EtcdServiceWatcher extends ServiceWatcher implements RangeCache.Listener {

        private final RangeCache rangeCache;
        private final EtcdClient client;
        private final ByteString prefix;
        private final int prefixLen;
        private final String serviceName;

        public EtcdServiceWatcher(String serviceName) throws Exception {
            super(serviceName);
            if (serviceName.indexOf('/') >= 0) {
                throw new IllegalArgumentException("service name must not contain '/'");
            }
            this.serviceName = serviceName;
	        this.prefix = EtcdDiscovery.servicePrefix(litelinksPrefix, serviceName);
	        this.prefixLen = prefix.size();
	        this.client = etcdConfig.getClient();
	        this.rangeCache = new RangeCache(client, prefix);
        }

        @Override
        protected ServiceRegistryClient getRegistryClient() {
            return EtcdServiceRegistry.this;
        }

        @Override
        protected ListenableFuture<Void> startAsync() {
            rangeCache.addListener(this);
            ListenableFuture<Boolean> cacheFuture = rangeCache.start();
            Futures.addCallback(cacheFuture, (FutureListener<Boolean>) (b, t) -> {
                if (t != null) {
                    rangeCache.close();
                    logger.error("RangeCache start failed for service " + serviceName);
                } else {
                    logger.info("started RangeCache for service " + serviceName);
                }
            }, directExecutor());
            return Futures.transform(cacheFuture, b -> null, directExecutor());
        }

        @Override
        public void close() {
            rangeCache.close();
        }

        @Override
        public boolean isValid() {
            return !rangeCache.isClosed() && !client.isClosed();
        }

        // collect the initial instances to publish as a single event
        protected List<Server> preInit = new ArrayList<>();

        @Override
        public void event(EventType type, KeyValue keyValue) {
            try {
                ByteString key = keyValue != null? keyValue.getKey() : null;
                if (logger.isDebugEnabled()) {
                    logger.debug("Etcd event: " + type
                                 + " key=" + (key != null? key.toStringUtf8() : "null"));
                }
                if (type == EventType.INITIALIZED) {
                    if (!preInit.isEmpty()) {
                        getListener().refreshServerList(preInit
                                .toArray(new Server[preInit.size()]));
                    }
                    preInit = null;
                    return;
                }
                if (key == null || key.size() <= prefixLen) {
                    logger.warn("Event " + type + " for unexpected key: "
                                + (key != null? key.toStringUtf8() : "null"));
                    return;
                }
                String instance = key.substring(prefixLen).toStringUtf8();
                if (preInit != null) {
                    if (type == EventType.UPDATED) {
                        Server server = getServer(instance, keyValue.getValue());
                        if (server != null) {
                            preInit.add(server);
                        }
                    }
                    return;
                }
                if (type == EventType.UPDATED) {
                    addChild(instance, keyValue.getValue());
                } else if (type == EventType.DELETED) {
                    getListener().serverRemoved(instance);
                } else {
                    logger.warn("Unrecognized cache event type: " + type);
                }
            } catch (RuntimeException rte) {
                logger.error("Exception while processing etcd event", rte);
            }
        }

        protected void addChild(String instance, ByteString data) {
            final Listener.Server svr = getServer(instance, data);
            if (svr != null) {
                getListener().serverAdded(svr.hostname, svr.port,
                        svr.registrationTime, svr.version, svr.key, svr.instanceId, svr.connConfig);
            }
        }

        private Listener.Server getServer(String key, ByteString data) {
            if (data != null && !data.isEmpty()) {
                try (InputStream is = data.newInput()) {
                    Listener.Server sv = InstanceRecord.SERIALIZER.deserialize(is);
                    if (sv.hostname == null || sv.hostname.isEmpty()) {
                        throw new Exception("null or empty hostname");
                    }
                    sv.key = key;
                    return sv;
                } catch (Exception e) {
                    logger.error("ignoring new registration " + key + " with data '"
                                 + data.toStringUtf8() + "' for service "
                                 + serviceName + " due to error", e);
                }
            } else {
                logger.error("ignoring new registration "
                             + key + " with null or empty data for service " + serviceName);
            }
            return null;
        }

        @Override
        protected boolean isAvailable() {
            return rangeCache.size() > 0;
        }

        @Override
        protected boolean doConfirmUnavailable() {
            // direct request to get count
            long count = client.getKvClient().get(prefix).asPrefix().countOnly()
                    .timeout(CONF_UNAVAILABLE_TIMEOUT_MS).sync().getCount();
            // In nonzero (available) case, we need to ensure the cache contains
            // the discovered entries before returning; strongIterator() achieves this.
            return count == 0 || Iterators.size(rangeCache.strongIterator()) == 0;
        }

        @Override
        protected void finalize() throws Throwable {
            close();
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(etcdConfig.getEndpoints(), etcdConfig.getRootPrefix());
    }

    @Override
    public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null || getClass() != obj.getClass()) return false;
        EtcdClusterConfig other = ((EtcdServiceRegistry) obj).etcdConfig;
        return Objects.equals(etcdConfig.getEndpoints(), other.getEndpoints())
               && Objects.equals(etcdConfig.getRootPrefix(), other.getRootPrefix());
    }
}
