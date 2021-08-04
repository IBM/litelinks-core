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

package com.ibm.watson.litelinks.test;

import com.ibm.watson.litelinks.server.ServiceDeploymentInfo;
import com.ibm.watson.litelinks.server.ThriftService;
import com.ibm.watson.litelinks.test.thrift.DummyService;
import com.ibm.watson.litelinks.test.thrift.DummyStruct;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;

public class SDITestingThriftServiceImpl extends ThriftService implements DummyService.Iface {

    public static volatile boolean provideVersion, provideName;

    private int initPubPort = -2;
    private String initInstId, initPubAddr, initSvcName, initSvcVersion;
    private SocketAddress initListenAddr;

    @Override
    protected TProcessor initialize() throws Exception {
        ServiceDeploymentInfo sdi = getInstanceDeploymentInfo();
        initPubPort = sdi.getPublicPort();
        initInstId = sdi.getInstanceId();
        initPubAddr = sdi.getPublicAddress();
        initListenAddr = sdi.getListeningAddress();
        initSvcName = sdi.getServiceName();
        initSvcVersion = sdi.getServiceVersion();

        return new DummyService.Processor<DummyService.Iface>(this);
    }

    @Override
    public String serviceVersion() {
        return provideVersion? "impl-provided-vers" : super.serviceVersion();
    }

    @Override
    public String defaultServiceName() {
        return provideVersion? "impl-provided-name" : super.defaultServiceName();
    }

    @Override
    public String method_one(String arg1, DummyStruct arg2, boolean arg3)
            throws TException {
        ServiceDeploymentInfo sdi = getInstanceDeploymentInfo();
        SocketAddress la = sdi.getListeningAddress();
        return initSvcName + "__" + initSvcVersion
               + "__" + initInstId + "|"
               + initPubPort + "__" + initPubAddr
               + "__" + initListenAddr + "|"
               + sdi.getPublicPort() + "__"
               + sdi.getServiceName() + "__"
               + sdi.getServiceVersion() + "__" +
               (la != null? la.getClass().getName() : "null") + "__" +
               (la instanceof InetSocketAddress
                       ? ((InetSocketAddress) la).getPort() : "-99") + "__" + la;
    }

    @Override
    public DummyStruct method_two(int arg1, String arg2, ByteBuffer arg3)
            throws TException {
        return new DummyStruct();
    }
}
