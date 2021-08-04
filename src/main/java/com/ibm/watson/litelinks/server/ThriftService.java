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

import com.google.common.util.concurrent.AbstractService;
import com.ibm.watson.litelinks.InvalidThriftClassException;
import com.ibm.watson.litelinks.MethodInfo;
import org.apache.thrift.TProcessor;

import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * Service implementations should extend this class and must override either {@link #initialize()} for
 * synchronous initialization or {@link #doStart()} for asynchronous initialization. In the latter
 * case, either {@link #notifyStarted(TProcessor)} or {@link #notifyFailed(Throwable)} must be
 * subsequently called by another thread.
 * <p>
 * {@link #shutdown()} or {@link #doStop()} may be optionally overridden to provide shutdown logic. The
 * latter is for asynchronous implementations and must be accompanied by a subsequent call to
 * {@link #notifyStopped()} or {@link #notifyFailed(Throwable)}.
 * <p>
 * The service should also call {@link #notifyFailed(Throwable)} while running if it enters a fatal
 * failure state (after which the shutdown method <b>won't</b> be invoked).
 * <p>
 * Finally, the subclass must have a default or no-arg constructor.
 */
public abstract class ThriftService extends AbstractService {

    private TProcessor tproc;

    private ServiceDeploymentInfo deploymentInfo;

    final TProcessor getTProcessor() {
        return tproc;
    }
    final void setDeploymentInfo(ServiceDeploymentInfo info) {
        this.deploymentInfo = info;
    }

    /**
     * This method should be overridden to initialize the
     * service implementation and return the thrift
     * TProcessor to use for the server interface.
     *
     * @throws Exception
     */
    protected TProcessor initialize() throws Exception {
        final Constructor<? extends TProcessor> tprocConstr = AdapterThriftService.getTProcConstructor(getClass());
        if (tprocConstr == null) {
            throw new InvalidThriftClassException("ThriftService " + getClass()
                    + " must implement the initialize() method or implement a thrift service Iface");
        }
        return tprocConstr.newInstance(this);
    }

    /**
     * This method can be optionally overridden to perform
     * shutdown tasks.
     */
    protected void shutdown() {
        // default is no-op
    }

    /**
     * This method can be optionally overridden to perform
     * tasks after a stop is triggered but prior to service
     * deregistration occurring.
     * <p>
     * It <b>won't</b> be called if the service is not
     * already registered.
     */
    protected void preShutdown() {
        // default is no-op
    }

    /**
     * Optionally override to provide liveness status.
     *
     * @return false if the service is in an unhealthy state and
     * should be restarted, true otherwise
     */
    protected boolean isLive() {
        return true;
    }

    /**
     * Optionally override to provide readiness status. Note
     * that this method will only be called post-initialization,
     * prior to that the service is always advertised as
     * <b>not</b> ready.
     *
     * @return true if the service should be advertised
     * as ready to serve requests, false otherwise
     */
    protected boolean isReady() {
        return true;
    }

    /**
     * This can be overridden for asynchronous use as an
     * alternative to implementing {@link #initialize()}
     */
    @Override
    protected /*final*/ void doStart() {
        try {
            tproc = initialize();
            notifyStarted();
        } catch (Throwable t) {
            notifyFailed(t);
        }
    }

    /**
     * This can be overridden for asynchronous use as an
     * alternative to implementing {@link #shutdown()}
     */
    @Override
    protected /*final*/ void doStop() {
        try {
            shutdown();
            notifyStopped();
        } catch (Throwable t) {
            notifyFailed(t);
        }
    }


    /**
     * Give notification that the service is started, and provide
     * the thrift TProcessor to use for serving requests
     * (which wraps the service interface implementation)
     *
     * @param tp
     */
    protected final void notifyStarted(TProcessor tp) {
        //TODO only allow to set it once
        tproc = tp;
        notifyStarted();
    }

    /**
     * Optional. If used (overridden), the name should be
     * available via this method prior to {@link #initialize()} returning
     * or {@link #notifyStarted(TProcessor)} being invoked
     *
     * @return optionally, a suggested service name which may be overridden
     * by the service deployer
     */
    public String defaultServiceName() {
//      return null;
        return getDefaultServiceName();
    }

    /**
     * Optional, override to provide. Value may be overridden by
     * the service deployer.
     *
     * @return the version of this service, or null if not specified
     */
    public String serviceVersion() {
//      return null;
        return getServiceVersion();
    }

    /**
     * Optional, override to provide method-specific configuration.
     * <p>
     * If the returned map contains an entry with key {@link MethodInfo#DEFAULT},
     * the corresponding config will be assumed for all methods which don't have
     * their own entry. The method info can also be specified
     * via annotations (see javadoc on {@link MethodInfo}, but method-specific
     * entries provided in this map take precedence.
     *
     * @return map whose keys correspond to method names of this services interface
     */
    public Map<String, MethodInfo> provideMethodInfo() {
        return null;
    }

    /**
     * Optional, override to provide a map of custom metadata which will be
     * associated with this service instance's registration and available to clients.
     *
     * @return map of custom metadata parameters
     */
    public Map<String, String> provideInstanceMetadata() {
        return null;
    }

    /**
     * Optional, override to provide a custom {@link ExecutorService} to use
     * for server processing of requests. It's recommended have this use
     * {@link ServerRequestThread}s if possible, and it is the service
     * implementation's responsibility to shutdown the executor (typically
     * within the {@link #shutdown()} method implementation.
     *
     * @return {@link ExecutorService} or null to use litelinks' default
     */
    public ExecutorService provideRequestExecutor() {
        return null;
    }

    /**
     * For debugging/instrumentation purposes. Note that certain values returned
     * by the getters of the provided {@link ServiceDeploymentInfo} instance
     * could change between initialization and runtime
     *
     * @return object holding various parameters/attributes related
     * to the current deployment of this service instance
     */
    protected final ServiceDeploymentInfo getInstanceDeploymentInfo() {
        ServiceDeploymentInfo sdi = deploymentInfo;
        if (sdi == null) {
            throw new IllegalStateException(
                    "Deployment info not available prior to service initialization");
        }
        return sdi;
    }

    // deprecated methods below here

    /**
     * @deprecated you should instead override {@link #defaultServiceName()}
     */
    @Deprecated
    public String getDefaultServiceName() {
        return null;
    }

    /**
     * @deprecated you should instead override {@link #serviceVersion()}
     */
    @Deprecated
    public String getServiceVersion() {
        return null;
    }

    /**
     * For debugging use only, query the port that this service is currently
     * listening on.
     *
     * @return the port number or 0 if yet to be assigned
     * @deprecated use {@link #getInstanceDeploymentInfo()} instead
     */
    @Deprecated
    protected final int getListeningPort() {
        SocketAddress addr = getInstanceDeploymentInfo().getListeningAddress();
        return addr instanceof InetSocketAddress? ((InetSocketAddress) addr).getPort() : 0;
    }
}
