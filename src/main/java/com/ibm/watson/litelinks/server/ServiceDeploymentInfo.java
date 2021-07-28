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

import java.net.SocketAddress;

/**
 * Provides server-side information about a service instances
 * deployment configuration
 */
public interface ServiceDeploymentInfo {

    /**
     * @return the logical service name (if already established)
     */
    String getServiceName();

    /**
     * @return the service version (if already established), or null
     */
    String getServiceVersion();

    /**
     * @return the instanceId for this service instance
     */
    String getInstanceId();

    //TODO may need a bit of abstraction here later
    // stuff below is largely specific to RPC/discovery
    // mechanism in use

    /**
     * NOTE if the port associated with the returned address is 0,
     * it means that it has not yet been assigned and will be available
     * once the server has started
     *
     * @return the local address which this service instance is
     * listening on (typically an {@link java.net.InetSocketAddress})
     */
    SocketAddress getListeningAddress();

    /**
     * @return the address/hostname used by clients to access
     * this service instance
     */
    String getPublicAddress();

    /**
     * NOTE if 0 is returned it means that the public port will be the
     * same as the listening port which has not yet been assigned. It
     * will be available once the server has started
     *
     * @return the port used by clients to access this service instance
     */
    int getPublicPort();

    /**
     * @return the address/hostname used in preference by "local" clients
     * to access this service instance, or null if a private address is
     * not configured
     */
    String getPrivateAddress();

    /**
     * NOTE if 0 is returned it means that the private port will be the
     * same as the listening port which has not yet been assigned. It
     * will be available once the server has started
     *
     * @return the port used by clients to access this service instance,
     * or -1 if not a private address is not configured
     */
    int getPrivatePort();

    /**
     * @return the private domain id that the deployed service instance
     * belongs to, or null if none is configured. Only applicable when
     * private endpoints are in play (see {@link #getPrivateAddress()})
     */
    String getPrivateDomain();

    //TBD
//	Map<String,String> getConnectionProperties();
//	SSLMode getSSLMode();
//	String getServiceImplClassName();
//	String getZookeeperConnString();
}
