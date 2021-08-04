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

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.SettableFuture;
import com.ibm.watson.litelinks.LitelinksExceptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Interface for service discovery clients - represents
 * the service registry associated with a particular logical service.
 */
@FunctionalInterface
public interface ServiceRegistryClient {

    /**
     * Create a new {@link ServiceWatcher} of this registry
     *
     * @param serviceName the logical service name to watch
     */
    ServiceWatcher newServiceWatcher(String serviceName) throws Exception;

    // Force implementations to override these
    @Override
    boolean equals(Object o);

    @Override
    int hashCode();

    /**
     * Watches a specific logical service which is registered in the registry
     */
    abstract class ServiceWatcher implements Closeable {
        private static final Logger logger = LoggerFactory.getLogger(ServiceWatcher.class);

        protected final String serviceName;
        RegistryListener listener;

        protected ServiceWatcher(String serviceName) {
            this.serviceName = serviceName;
        }

        protected abstract ServiceRegistryClient getRegistryClient();

        protected Listener getListener() {
            return listener;
        }

        /**
         * Called by the consumer of this {@link ServiceWatcher}
         *
         * @param listener
         * @param timeoutMillis
         * @throws Exception
         */
        public final void start(RegistryListener listener, long timeoutMillis)
                throws Exception { //TODO exception types TBD
            this.listener = listener;
            start(timeoutMillis);
        }

        public final ListenableFuture<Void> startAsync(RegistryListener listener) {
            this.listener = listener;
            return startAsync();
        }

        /**
         * Perform any necessary initialization (such as connecting to the service registry)
         * and retrieve initial service state, which should be set via the {@link Listener} methods.
         *
         * @param timeoutMillis initialization should timeout after this time
         * @throws Exception
         */
        protected void start(long timeoutMillis) throws Exception { //TODO exception types TBD
            Future<Void> startFuture = startAsync();
            try {
                startFuture.get(timeoutMillis, TimeUnit.MILLISECONDS);
            } catch (InterruptedException | TimeoutException e) {
                startFuture.cancel(true);
                throw e;
            } catch (ExecutionException ee) {
                Throwables.throwIfInstanceOf(ee.getCause(), Exception.class);
                Throwables.throwIfUnchecked(ee.getCause());
                throw ee;
            }
        }

        protected abstract ListenableFuture<Void> startAsync();

        /**
         * Disconnect/stop listening for service events
         */
        @Override
        public abstract void close();

        /**
         * Optionally overridden
         *
         * @return true if this watcher hasn't been closed/disconnected
         */
        public boolean isValid() {
            return true;
        }

        // rate-limit remote confirm-unavailability attempts to one every 2.5 secs
        private final RateLimiter limiter = RateLimiter.create(0.4);
        private final AtomicReference<ListenableFuture<Boolean>> unavailabilityCheckFuture
                = new AtomicReference<>();

        /**
         * This method is used to confirm that there are no available service
         * instances <b>in a synchronized fashion</b>.
         * The service registry will be checked directly and the method will return
         * true if the service is unavailable. In the false (available) case only,
         * all corresponding server addition/removal callbacks are guaranteed to
         * have been completed prior to the method returning (but not necessarily
         * from the same thread).
         * <p>
         * Should be used only as confirmation of unavailability and not
         * for general availability checks.
         * <p>
         * Implementations should return true if such synchronous confirmation
         * is not possible or a problem is encountered obtaining it in a timely manner.
         *
         * @return true if service is unavailable, false otherwise
         */
        public boolean confirmUnavailable() {
            if (isAvailable()) {
                return false;
            }
            SettableFuture<Boolean> newFut = null;
            boolean firstWait = true;
            do {
                ListenableFuture<Boolean> fut = unavailabilityCheckFuture.get();
                if (fut != null) {
                    try {
                        boolean result = fut.get();
                        if (!result | !firstWait) {
                            return result;
                        }
                        if (isAvailable()) {
                            return false;
                        }
                        firstWait = false;
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return true;
                    } catch (ExecutionException ee) {
                        logger.warn("Exception confirming unavailability of service: "
                                    + serviceName, ee.getCause());
                        return true;
                    }
                }
                // rate-limit new attempts to the server
                if (!limiter.tryAcquire()) {
                    return true;
                }
                if (newFut == null) {
                    newFut = SettableFuture.create();
                }
            } while (!unavailabilityCheckFuture.compareAndSet(null, newFut));

            boolean unavailable = true;
            Throwable failure = null;
            try {
                unavailable = !isAvailable() && doConfirmUnavailable();
            } catch (Throwable t) {
                failure = t;
                if (t instanceof Error) {
                    throw t;
                }
                if (LitelinksExceptions.isInterruption(t)) {
                    Thread.currentThread().interrupt();
                } else {
                    logger.warn("Exception confirming unavailability of service: "
                                + serviceName, t);
                }
            } finally {
                unavailabilityCheckFuture.set(null);
                if (failure != null) {
                    newFut.setException(failure);
                } else {
                    newFut.set(unavailable);
                }
            }
            return unavailable;
        }

        protected abstract boolean isAvailable();

        protected boolean doConfirmUnavailable() {
            return true;
        }
    }


    /**
     * {@link ServiceWatcher} implementations use this interface to
     * make service state change notifications (add/remove cluster members,
     * conn config changes)
     * <p>
     * <b>IMPORTANT:</b> It is currently assumed that all notifications
     * for a given service registry will come from a single thread.
     */
    interface Listener {
        class Server {
            public String key; // (effectively final)
            public final String hostname, instanceId;
            public final int port;
            public final String version;
            public final long registrationTime;
            public final Map<Object, Object> connConfig;

            public Server(String hostname, int port, long registrationTime,
                    String version, String key, String instanceId,
                    Map<Object, Object> connConfig) {
                this.hostname = hostname;
                this.port = port;
                this.version = version;
                this.registrationTime = registrationTime;
                this.key = key;
                this.instanceId = instanceId;
                this.connConfig = connConfig;
            }
        }

        /**
         * A new service instance has become active
         *
         * @param hostname
         * @param port
         * @param version    the service-specific version of this instance, may be null
         * @param key        unique key corresponding to the instance
         * @param instanceId arbitrary id, usually but not necessarily unique; may be equal to key
         * @param connConfig properties specifying how to connect to this instance
         */
        void serverAdded(String hostname, int port, long registrationTime, String version,
                String key, String instanceId, Map<Object, Object> connConfig);

        /**
         * A service instance was removed/deactivated
         *
         * @param key
         */
        void serverRemoved(String key);

        /**
         * Method for bulk add/remove operations (can be used in place of
         * {@link #serverAdded(String, int, long, String, String, String, Map)}
         * and/or {@link #serverRemoved(String)}.
         *
         * @param servers
         * @param removedIds
         */
        void serversAddedAndOrRemoved(Server[] servers, String[] removedIds);

        /**
         * Absolute replacement of all known service instances.
         *
         * @param servers
         */
        void refreshServerList(Server[] servers);
    }

    abstract class RegistryListener implements Listener {
        abstract void serverAdded(String hostname, int port, long registrationTime, String version,
                String key, String instanceId, Map<Object, Object> connConfig, ServiceRegistryClient source);

        abstract void serversAddedAndOrRemoved(Server[] servers,
                String[] removedIds, ServiceRegistryClient source);

        abstract void refreshServerList(Server[] servers, ServiceRegistryClient source);

        @Override
        public void serverAdded(String hostname, int port, long registrationTime,
                String version, String key, String instanceId, Map<Object, Object> connConfig) {
            serverAdded(hostname, port, registrationTime, version, key, instanceId, connConfig, null);
        }
        @Override
        public void serversAddedAndOrRemoved(Server[] servers, String[] removedIds) {
            serversAddedAndOrRemoved(servers, removedIds, null);
        }
        @Override
        public void refreshServerList(Server[] servers) {
            refreshServerList(servers, null);
        }
    }
}
