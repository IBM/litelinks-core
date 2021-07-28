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
import com.ibm.etcd.client.FutureListener;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Stream;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

/**
 * WARNING: currently assumes/requires service instance key uniqueness
 * between all the registries.
 */
public class CompositeServiceRegistry implements ServiceRegistryClient {

    protected final ServiceRegistryClient[] members;

    public CompositeServiceRegistry(ServiceRegistryClient[] members) {
        this.members = members;
    }

    public CompositeServiceRegistry(Collection<ServiceRegistryClient> members) {
        this.members = members.toArray(new ServiceRegistryClient[members.size()]);
    }

    @Override
    public ServiceWatcher newServiceWatcher(String serviceName) throws Exception {

        ServiceWatcher[] watchers = new ServiceWatcher[members.length];
        for (int i = 0; i < members.length; i++) {
            try {
                watchers[i] = members[i].newServiceWatcher(serviceName);
            } catch (Exception e) {
            for (ServiceWatcher sw : watchers) if (sw != null) sw.close();
                throw e;
            }
        }
        return new CompositeServiceWatcher(serviceName, watchers);
    }

    final class CompositeServiceWatcher extends ServiceWatcher { //TODO static TBD
        final ServiceWatcher[] watchers;

        public CompositeServiceWatcher(String serviceName, ServiceWatcher[] watchers) {
            super(serviceName);
            this.watchers = watchers;
        }

        @Override
        protected ServiceRegistryClient getRegistryClient() {
            return CompositeServiceRegistry.this;
        }

        @Override
        protected ListenableFuture<Void> startAsync() {
            // considered started when all delegates are started
            ListenableFuture<Void> future = Futures.transform(Futures.allAsList(
                    (Iterable<ListenableFuture<Void>>) Arrays.stream(watchers)
                            .map(sw -> sw.startAsync(new RegistryListener() {
                                @Override
                                public void serverRemoved(String key) {
                                    listener.serverRemoved(key);
                                }

                                @Override
                                public void serversAddedAndOrRemoved(Server[] servers, String[] removedIds,
                                        ServiceRegistryClient source) {
                                    listener.serversAddedAndOrRemoved(servers, removedIds, sw.getRegistryClient());
                                }

                                @Override
                                void serverAdded(String hostname, int port, long registrationTime, String version,
                                        String key,
                                        String instanceId, Map<Object, Object> connConfig,
                                        ServiceRegistryClient source) {
                                    listener.serverAdded(hostname, port, registrationTime, version,
                                            key, instanceId, connConfig, sw.getRegistryClient());
                                }

                                @Override
                                void refreshServerList(Server[] servers, ServiceRegistryClient source) {
                                    listener.refreshServerList(servers, sw.getRegistryClient());
                                }
                            }))::iterator), l -> null, directExecutor());
            Futures.addCallback(future, (FutureListener<Void>) (v, t) -> {
                if (t != null) {
                    close(); // ensure all or nothing
                }
            }, directExecutor());
            return future;
        }

        @Override
        protected boolean isAvailable() {
            return Stream.of(watchers).anyMatch(ServiceWatcher::isAvailable);
        }

        @Override
        public boolean confirmUnavailable() {
            //TODO parallelize
            return Stream.of(watchers).allMatch(ServiceWatcher::confirmUnavailable);
        }

        @Override
        public boolean isValid() {
            for (ServiceWatcher sw : watchers) {
                if (!sw.isValid()) return false;
            }
            return true;
        }

        @Override
        public void close() {
            //TODO parallelize probably`
            for (ServiceWatcher sw : watchers) {
                sw.close();
            }
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(watchers);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof CompositeServiceWatcher
                   && Arrays.equals(watchers,
                    ((CompositeServiceWatcher) obj).watchers);
        }

    }

}
