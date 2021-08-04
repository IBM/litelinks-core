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

import com.ibm.watson.litelinks.test.thrift.DummyService;
import com.ibm.watson.litelinks.test.thrift.DummyStruct;
import org.apache.thrift.TException;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

public class SimpleThriftServiceImpl implements DummyService.Iface {

    public static volatile boolean isRunning;

    public SimpleThriftServiceImpl() {
        System.out.println("initialize logic here");
        isRunning = true;
    }

    private final DummyService.Iface delegate = new TestThriftServiceImpl();

    @Override
    public String method_one(String arg1, DummyStruct arg2, boolean arg3)
            throws TException {
        return delegate.method_one(arg1, arg2, arg3);
    }

    @Override
    public DummyStruct method_two(int arg1, String arg2, ByteBuffer arg3)
            throws TException {
        return delegate.method_two(arg1, arg2, arg3);
    }

    public static class WithShutdown extends SimpleThriftServiceImpl implements Closeable {

        public static boolean isShutdown;

        @Override
        public void close() throws IOException {
            System.out.println("shutdown logic here");
            isShutdown = true;
        }
    }

}
