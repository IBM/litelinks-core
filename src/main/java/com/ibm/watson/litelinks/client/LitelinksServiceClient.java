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

import com.ibm.watson.litelinks.ThreadContext;

import java.io.Closeable;
import java.util.List;
import java.util.Map;

/**
 * Interface implemented by clients returned by {@link ThriftClientBuilder}.
 * Can be used to check or wait for availability of the corresponding
 * service.
 */
public interface LitelinksServiceClient extends Closeable {

    /**
     * Block until service is available or specified timeout expires.
     * A timeout of 0 can be used to check if the service is currently
     * available.
     *
     * @param timeoutMillis
     * @return true if the service is available, false otherwise
     */
    boolean awaitAvailable(long timeoutMillis) throws InterruptedException;

    /**
     * Equivalent to calling {@link #awaitAvailable(long)} with 0L.
     */
    boolean isAvailable();

    /**
     * Test service connectivity. This attempts to 'ping' the service and returns
     * without throwing an exception if successful.
     */
    void testConnection()
            throws Exception;

    /**
     * Test service connectivity. This attempts to 'ping' the service and returns
     * without throwing an exception if successful.
     *
     * @param timeoutMillis maximum time to wait in milliseconds
     * @throws Exception includes (possibly conn type-specific) timeout exceptions
     */
    void testConnection(long timeoutMillis)
            throws Exception;

    /**
     * Returns information about the currently registered service instances
     * <p>
     * This is intended for instrumentation / administrative use only and shouldn't be called
     * at a high frequency or used for routing decisions.
     */
    List<ServiceInstanceInfo> getServiceInstanceInfo();

    interface ServiceInstanceInfo {
        String getHost();

        int getPort();

        String getInstanceId();

        String getVersion(); // may be null

        boolean isActive();

        long getRegistrationTime();

        /**
         * @return custom metadata parameters published by this service instance
         */
        Map<String, String> getMetadata();

        /**
         * Test service connectivity. Attempts to 'ping' this service instance
         * and returns without throwing an exception if successful.
         * <p>
         * <b>IMPORTANT:</b> Use {@link LitelinksServiceClient#testConnection(long)} to
         * test connectivity to the service as a whole. This method may fail if
         * this specific service instance was legitimately shutdown/deregistered.
         *
         * @param timeoutMillis maximum time to wait in milliseconds
         * @throws Exception includes (possibly conn type-specific) timeout exceptions
         */
        void testConnection(long timeoutMillis) throws Exception;
    }

    /**
     * Obtain a "view" of this client which will include the specified context
     * parameters on all method calls (as if set via {@link ThreadContext}),
     * regardless of the executing thread. Parameters set via this method take precedence
     * over both those set using <code>ThreadContext</code> directly and those set
     * via {@link ThriftClientBuilder#withContext(Map)} or
     * {@link ThriftClientBuilder#withContextEntry(String, String)}.
     * <p>
     * Suitable for "one-time" use but setting <code>ThreadContext</code> directly
     * is preferable for performance.
     *
     * @param contextArgs parameters to add to all methods invoked on the returned proxy
     * @return a proxy wrapper around this client
     * @see ThreadContext
     */
    <I> I contextProxy(Map<String, String> contextArgs);

    /**
     * Close this client instance. This may not close the underlying resources immediately
     * if they are being cached for reuse and/or shared by other client instances.
     */
    @Override
    void close();
}
