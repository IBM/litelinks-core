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
import com.ibm.watson.litelinks.test.thrift.DummyService2;
import org.apache.thrift.TException;

/**
 * Just for unit testing
 */
public class TestThriftService2Impl extends ThriftService implements DummyService2.Iface {

    @Override
    public String just_one_method(String input) throws TException {
        return "testonly";
    }

}
