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

import com.ibm.watson.litelinks.server.RequestListener.BaseRequestListener;
import com.ibm.watson.litelinks.server.ServiceDeploymentInfo;

import java.util.Map;

public class TestRequestListener extends BaseRequestListener {

    static volatile String initExceptionMsg;
    static volatile TestRequestListener lastInst;

    final String thisInitExceptionMsg;

    volatile String thisNewReqExceptionMsg;

    String iName, iInstId;

    volatile String nMethod, nParam;
    volatile Object nRemoteAddress;
    volatile Object nSslSession;

    volatile String cMethod, cParam;
    volatile Object cRemoteAddress;

    public TestRequestListener() {
        thisInitExceptionMsg = initExceptionMsg;
        lastInst = this;
    }

    @Override
    public void initialize(ServiceDeploymentInfo deploymentInfo) throws Exception {
        System.out.println("req listener init called with " + deploymentInfo);
        iName = deploymentInfo.getServiceName();
        iInstId = deploymentInfo.getInstanceId();
        if (thisInitExceptionMsg != null) {
            throw new Exception(thisInitExceptionMsg);
        }
    }

    @Override
    public Object newRequest(String method, Map<String, String> context,
            Map<String, Object> transportParams) throws Exception {
        System.out.println("newreq for method " + method + ", context="
                           + context + ", tparms=" + transportParams);
		this.nMethod = method;
		this.nParam = context != null ? context.get("param") : null;
		this.nRemoteAddress = transportParams != null ? transportParams.get(TP_REMOTE_ADDRESS) : null;
		this.nSslSession = transportParams != null ? transportParams.get(TP_SSL_SESSION) : null;
        String nrem = thisNewReqExceptionMsg;
        if (nrem != null) {
            throw new Exception(nrem);
        }
        return null;
    }

    @Override
    public void requestComplete(String method, Map<String, String> context,
            Map<String, Object> transportParams, Throwable failure,
            Object handle) {
        System.out.println("reqcomplete for method " + method
                           + ", context=" + context + ", tparms=" + transportParams
                           + ", failure=" + failure + ", handle=" + handle);
		this.cMethod = method;
		this.cParam = context != null ? context.get("param") : null;
		this.cRemoteAddress = transportParams != null ? transportParams.get(TP_REMOTE_ADDRESS) : null;
    }

    public static class ThrowingRequestListener extends TestRequestListener {
        @Override
        public void initialize(ServiceDeploymentInfo deploymentInfo) throws Exception {
            super.initialize(deploymentInfo);
            thisNewReqExceptionMsg = "INSTANCE ID IS " + iInstId;
        }
    }
}
