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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import com.ibm.etcd.client.EtcdClient;
import com.ibm.etcd.client.config.EtcdClusterConfig;
import com.ibm.etcd.client.utils.PersistentLeaseKey;
import com.ibm.watson.litelinks.ThriftConnProp;
import com.ibm.watson.litelinks.server.ConfiguredService;
import com.ibm.watson.litelinks.server.WatchedService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

public class EtcdWatchedService extends WatchedService {

    private static final Logger logger = LoggerFactory.getLogger(EtcdWatchedService.class);

    private final String etcdConfigFile;

    private EtcdClient client; // final post-init
    private ByteString rootPrefix; // final post-init

    private volatile PersistentLeaseKey plk;

    public EtcdWatchedService(Service monitoredService, String etcdConfigFile,
            String serviceName, String serviceVersion, String instanceId, int port, int probePort) {
        super(monitoredService, serviceName, serviceVersion, instanceId, port, probePort);
        if (etcdConfigFile == null) {
            throw new IllegalArgumentException("must provide etcd config");
        }
        this.etcdConfigFile = etcdConfigFile;
    }

    @Override
    protected boolean deregister() {
        PersistentLeaseKey plk = this.plk;
        if (plk != null) {
            synchronized (plk) {
                if (this.plk != null) {
                    try {
                        logger.info("about to deregister service");
                        Futures.addCallback(plk.closeWithFuture(), new FutureCallback<Object>() {
                            @Override
                            public void onSuccess(Object result) {
                                logger.info("persistent service key closed successfully");
                            }
                            @Override
                            public void onFailure(Throwable t) {
                                logger.error("error closing persistent service key", t);
                            }
                        }, directExecutor());
                        this.plk = null;
                        return true;
                    } catch (Exception e) {
                        logger.warn("Exception closing persistent lease key", e);
                    }
                }
            }
        }
        return false;
    }

    @Override
    protected void initialize() throws Exception {
        EtcdClusterConfig etcdConfig = EtcdClusterConfig.fromJsonFileOrSimple(etcdConfigFile);
        client = etcdConfig.getClient();
        rootPrefix = etcdConfig.getRootPrefix();
        Futures.addCallback(client.getSessionLease(), new FutureCallback<Long>() {
            @Override
            public void onSuccess(Long id) {
                logger.info("etcd session lease established successfully with id " + id);
            }
            @Override
            public void onFailure(Throwable t) {
                logger.error("etcd session lease establishment failed", t);
                failedWhileStarting(t);
            }
        }, directExecutor());
        logger.info("created etcd client and initiated session lease");
    }

    private static final int KEY_CREATE_TIMEOUT_SECS = 10;

    @Override
    protected void registerAsync() throws Exception {
        if (client == null) {
            throw new IllegalStateException("etcd client not found");
        }

        final String serviceName = getServiceName();
        if (serviceName == null) {
            throw new IllegalStateException("Could not determine name for service");
        }
        if (serviceName.indexOf('/') >= 0) {
            throw new IllegalArgumentException("service name must not contain '/'");
        }

        final String version = getServiceVersion();
        final String instanceId = getInstanceId();
        final String serviceHost = getHost();
        final int port = getPublicPort();
        final String privateEndpoint = getPrivateEndpointString();

        // kind of arbitrary, must be unique
        final String instanceUid = (serviceHost + "_" + port + "_" +
                                    Long.toHexString(System.currentTimeMillis()))
                .replace('/', '\\'); // forward-slash replacement just-in-case

        final ByteString serviceKey = EtcdDiscovery.instanceKey(rootPrefix,
                serviceName, instanceUid);

        logger.info("registering service in etcd: " + serviceKey.toStringUtf8() + ", endpoint "
                    + serviceHost + ":" + port + ", private endpoint "
                    + (privateEndpoint != null ? privateEndpoint : "none")
                    + ", version " + (version != null ? version : "not specified"));

        final ConfiguredService cservice = getConfiguredService();
        Map<String, String> config = cservice != null ? cservice.getConfig() : null;
        if (config == null) {
            config = Collections.emptyMap();
        }
        if (privateEndpoint != null) {
            config = new HashMap<>(config);
            config.put(ThriftConnProp.PRIVATE_ENDPOINT, privateEndpoint);
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        InstanceRecord srec = new InstanceRecord(serviceHost, port, System.currentTimeMillis(),
                version, null, instanceId, (Map<Object, Object>) (Map) config);

        plk = new PersistentLeaseKey(client, serviceKey,
                UnsafeByteOperations.unsafeWrap(InstanceRecord.SERIALIZER.serialize(srec)), null);

        logger.info("creating session-linked service key...");

        ListenableFuture<ByteString> startFuture = Futures.withTimeout(plk.startWithFuture(),
                KEY_CREATE_TIMEOUT_SECS, TimeUnit.SECONDS, eventThreads);

        Futures.addCallback(startFuture, new FutureCallback<ByteString>() {
            @Override
            public void onSuccess(ByteString key) {
                logger.info("etcd session key created/verified successfully: " + key.toStringUtf8());
                notifyStarted();
            }
            @Override
            public void onFailure(Throwable t) {
                logger.error("etcd session key creation failed", t);
                failedWhileStarting(t); // this will call deregister()
            }
        }, directExecutor());
    }

    @Override
    protected boolean isRegistered() {
        return plk != null;
    }
}
