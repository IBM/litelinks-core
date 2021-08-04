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

import com.ibm.watson.litelinks.server.ThriftService;
import com.ibm.watson.litelinks.test.thrift.DummyEnum;
import com.ibm.watson.litelinks.test.thrift.DummyService;
import com.ibm.watson.litelinks.test.thrift.DummyStruct;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Just for unit testing
 */
public class TestThriftServiceImpl extends ThriftService implements DummyService.Iface {

    boolean live = true, ready = true;

    @Override
    protected TProcessor initialize() throws Exception {
        return new DummyService.Processor<TestThriftServiceImpl>(this);
    }

    @Override
    protected boolean isLive() {
        return live;
    }

    @Override
    protected boolean isReady() {
        return ready;
    }

    @Override
    public String method_one(String arg1, DummyStruct arg2, boolean arg3) throws TException {
        if ("setlive".equals(arg1)) {
            live = arg3;
        } else if ("setready".equals(arg1)) {
            ready = arg3;
        }
        return "OK";
    }

    public final AtomicInteger hitCount = new AtomicInteger();

    @Override
    public DummyStruct method_two(int arg1, String arg2, ByteBuffer arg3) throws TException {
        long start = System.nanoTime();
        DummyStruct ds = new DummyStruct();
        ds.setBoolField(true);
        ds.setDoubleField(0.456d + arg1);
        ds.setEnumField(DummyEnum.OPTION2);
        ds.setListField(Arrays.asList("first", "second", arg2,
                StandardCharsets.UTF_8.decode(arg3.duplicate()).toString()));
        ds.setBinaryField(arg3.duplicate().asReadOnlyBuffer());
        ds.setMapField(Collections.emptyMap());
        ds.setStringField(null);
        ds.setRequiredIntField(-33);
        if (arg1 == 1) {
            try {
                Thread.sleep((long) (Math.random() * 500));
            } catch (InterruptedException e) {
                throw new TException(e);
            }
        }
        ds.setLongField(System.nanoTime() - start);
        hitCount.incrementAndGet();
        return ds;
    }

}
