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

import com.ibm.watson.litelinks.MethodInfo;
import com.ibm.watson.litelinks.server.ThriftService;
import com.ibm.watson.litelinks.test.thrift.DummyService;
import com.ibm.watson.litelinks.test.thrift.DummyService.Iface;
import org.apache.thrift.TProcessor;

import java.util.Map;

/**
 * Just for unit testing
 */
public class SettableThriftServiceImpl extends ThriftService { //implements Iface {

    public static volatile Iface template;

    public static volatile long initPause; // ms

    public static volatile long shutdownPause; // ms

    public static volatile Map<String, String> customParams;

    public static volatile Map<String, MethodInfo> mInfos;

    static {
        String ipause = System.getenv("svcinitpause");
        initPause = ipause != null? Long.parseLong(ipause) : 0l;

        String spause = System.getenv("svcshutdownpause");
        shutdownPause = spause != null? Long.parseLong(spause) : 0l;
    }

    private final long ipause, spause;

    public SettableThriftServiceImpl() {
        ipause = initPause;
        spause = shutdownPause;
    }

    @Override
    protected TProcessor initialize() throws Exception {
        if (ipause > 0) {
            Thread.sleep(ipause);
        }
        return new DummyService.Processor<Iface>(template);
    }

    @Override
    public Map<String, String> provideInstanceMetadata() {
        return customParams;
    }

    @Override
    public Map<String, MethodInfo> provideMethodInfo() {
        return mInfos;
    }


    @Override
    protected void shutdown() {
        if (spause > 0) {
            try {
                Thread.sleep(spause);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
