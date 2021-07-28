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

import java.net.InetSocketAddress;
import java.net.SocketAddress;

/**
 * This mutable {@link ServiceDeploymentInfo} impl is used to exchange
 * deployment-specific config between the service registration layer
 * ({@link WatchedService} impl) and the RPC server layer (e.g.
 * {@link DefaultThriftServer}). It is accessible in a read-only manner
 * from the service impl code and/or request listeners.
 */
// package-private
class SettableServiceDeploymentInfo implements ServiceDeploymentInfo {

    private String serviceName, serviceVersion, instanceId;

    private String publicAddress;
    private int publicPort;

    // optional - will be null and -1 respectively if not configured
    private String privateAddress;
    private int privatePort = -1;
    private String privateDomainId;

    private SocketAddress listeningAddress;

    @Override
    public String getServiceName() {
        return serviceName;
    }

    @Override
    public String getServiceVersion() {
        return serviceVersion;
    }

    @Override
    public String getInstanceId() {
        return instanceId;
    }

    @Override
    public SocketAddress getListeningAddress() {
        return listeningAddress;
    }

    @Override
    public String getPublicAddress() {
        return publicAddress;
    }

    @Override
    public int getPublicPort() {
        return getPublishedPort(publicPort);
    }

    @Override
    public String getPrivateAddress() {
        return privateAddress;
    }

    @Override
    public int getPrivatePort() {
        return getPublishedPort(privatePort);
    }

    @Override
    public String getPrivateDomain() {
        return privateDomainId;
    }

    private int getPublishedPort(int explicitOrZero) {
        if (explicitOrZero > 0) {
            return explicitOrZero;
        }
        final SocketAddress laddr = listeningAddress;
        return laddr instanceof InetSocketAddress?
                ((InetSocketAddress) laddr).getPort() : 0;
    }

//	@Override
//	public String getZookeeperConnString() {
//		// TODO Auto-generated method stub
//		return null;
//	}

    void setListeningAddress(SocketAddress listeningAddress) {
        this.listeningAddress = listeningAddress;
    }

    void setPublicAddress(String publicAddress) {
        this.publicAddress = publicAddress;
    }

    void setPublicPort(int publicPort) {
        this.publicPort = publicPort;
    }

    void setPrivateAddress(String privateAddress) {
        this.privateAddress = privateAddress;
    }

    void setPrivatePort(int privatePort) {
        this.privatePort = privatePort;
    }

    void setPrivateDomain(String privateDomainId) {
        this.privateDomainId = privateDomainId;
    }

    void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    void setServiceVersion(String serviceVersion) {
        this.serviceVersion = serviceVersion;
    }

    void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

}
