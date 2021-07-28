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

import com.ibm.watson.litelinks.ThreadContext;
import org.apache.thrift.TException;

import java.util.Map;

/**
 * Implementations of this interface can be provided to litelinks at deployment
 * time for instrumenting requests to the service instance.
 * It can also be used to intercept and selectively reject requests - e.g.
 * for authorization and/or authentication purposes.
 * <p>
 * Implementing classes are required to have a default or no-arg constructor,
 * and should extend {@link BaseRequestListener}.
 */
public interface RequestListener {

    /**
     * Remote {@link java.net.SocketAddress} (typically
     * {@link java.net.InetSocketAddress}), if available
     */
    String TP_REMOTE_ADDRESS = "TP_REMOTE_ADDRESS";

    /**
     * The connection's {@link javax.net.ssl.SSLSession}, if present
     */
    String TP_SSL_SESSION = "TP_SSL_SESSION";

    /**
     * Invoked during initialization of the service instance (might be before or
     * after initialization of the service instance itself). No other methods
     * will be called on this listener until this method has returned.
     *
     * @param deploymentInfo parameters related to the deployment
     *                       of this particular service instance
     * @throws Exception exceptions thrown by this method will cause initialization
     *                   of the service instance to be aborted
     */
    void initialize(ServiceDeploymentInfo deploymentInfo) throws Exception;

    /**
     * Invoked during shutdown of the service instance. This won't be called
     * until all other method invocations on this listener have completed.
     */
    void shutdown();

    /**
     * Called on every incoming API request, prior to the service method implementation
     * being invoked.
     * <p>
     * The request can be rejected by throwing an exception. Although not declared
     * explicitly (to accommodate possible future generalization), impls should
     * throw {@link TException}s for now.
     *
     * @param method          The name of the API method being called
     * @param context         The contents of the {@link ThreadContext} map for this request, or
     *                        null if none present
     * @param transportParams transport-level parameters associated with the request, as
     *                        available (such as remote address)
     * @return object which will be passed to the postRequest method;
     * should return null if unused
     * @throws Exception
     */
    Object newRequest(String method, Map<String, String> context,
            Map<String, Object> transportParams) throws Exception;

    /**
     * Called after every completed API request. The response has already been sent
     * to the caller at this stage.
     * Note that no explicit indication of success/failure or any information about
     * the response is currently provided.
     * <p>
     * Should not throw exceptions.
     *
     * @param method          The name of the API method being called
     * @param context         The contents of the {@link ThreadContext} map for this request, or
     *                        null if none present
     * @param transportParams transport-level parameters associated with the request, as
     *                        available (such as remote address)
     * @param failure         the exception thrown by a different invoked listener's newRequest
     *                        method. A non-null value means the service impl itself was <b>not</b> invoked
     * @param handle          object returned by the corresponding previously invoked
     *                        {@link #newRequest(String, Map, Map)} method
     */
    void requestComplete(String method, Map<String, String> context,
            Map<String, Object> transportParams, Throwable failure, Object handle);


    /**
     * Implementations should extend this adapter class instead of implementing the
     * interface directly to maintain forwards compatibility in case methods are added
     * in future.
     */
    abstract class BaseRequestListener implements RequestListener {
        @Override
        public void initialize(ServiceDeploymentInfo deploymentInfo) throws Exception {}

        @Override
        public void shutdown() {}

        @Override
        public Object newRequest(String method, Map<String, String> context,
                Map<String, Object> transportParams) throws Exception {
            return null;
        }

        @Override
        public void requestComplete(String method, Map<String, String> context,
                Map<String, Object> transportParams, Throwable failure, Object handle) {}
    }
}
