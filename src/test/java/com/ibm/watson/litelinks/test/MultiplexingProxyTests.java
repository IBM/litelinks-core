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

import com.google.common.util.concurrent.Service;
import com.ibm.watson.litelinks.client.LitelinksServiceClient;
import com.ibm.watson.litelinks.client.ServiceUnavailableException;
import com.ibm.watson.litelinks.client.ThriftClientBuilder;
import com.ibm.watson.litelinks.server.LitelinksService;
import com.ibm.watson.litelinks.server.MultiplexingProxyService;
import com.ibm.watson.litelinks.test.thrift.DummyService;
import com.ibm.watson.litelinks.test.thrift.DummyService2;
import com.ibm.watson.litelinks.test.thrift.DummyStruct;
import org.apache.curator.test.TestingServer;
import org.apache.thrift.TException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.*;

public class MultiplexingProxyTests {

    private static TestingServer localZk;
    public static String ZK;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        localZk = new TestingServer();
        ZK = localZk.getConnectString();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        if (localZk != null) {
            localZk.close();
        }
    }

    @Test
    public void test_proxy() throws Exception {
        Service sA = null, sB = null, sC = null, gway = null;
        synchronized (SettableThriftServiceImpl.class) {
            try {
                // start back-end services
                SettableThriftServiceImpl.initPause = 0l;
                SettableThriftServiceImpl.template = new DummyService.Iface() {
                    @Override
                    public DummyStruct method_two(int arg1, String arg2, ByteBuffer arg3)
                            throws TException { return null; }

                    @Override
                    public String method_one(String arg1, DummyStruct arg2, boolean arg3)
                            throws TException { return "serviceA-response"; }
                };
                sA = LitelinksService.createService(SettableThriftServiceImpl.class, ZK, "serviceA").startAsync();
                sA.awaitRunning(); // needed to ensure correct template is used!
                SettableThriftServiceImpl.template = new DummyService.Iface() {
                    @Override
                    public DummyStruct method_two(int arg1, String arg2, ByteBuffer arg3)
                            throws TException { return null; }

                    @Override
                    public String method_one(String arg1, DummyStruct arg2, boolean arg3)
                            throws TException { return "serviceB-response"; }
                };
                sB = LitelinksService.createService(SettableThriftServiceImpl.class, ZK, "serviceB").startAsync();
                sC = LitelinksService.createService(TestThriftService2Impl.class, ZK, "serviceC").startAsync();
                sB.awaitRunning();

                // start 'direct' clients & verify direct availability
                DummyService.Iface clientA = ThriftClientBuilder.newBuilder(DummyService.Iface.class)
                        .withZookeeper(ZK).withServiceName("serviceA").buildOnceAvailable(5000l);
                DummyService2.Iface clientC = ThriftClientBuilder.newBuilder(DummyService2.Iface.class)
                        .withZookeeper(ZK).withServiceName("serviceC").buildOnceAvailable(5000l);
                assertEquals("serviceA-response", clientA.method_one(null, null, false));
                assertEquals("testonly", clientC.just_one_method(null));

                // start first proxy client and verify unavailable
                DummyService.Iface clientAprox = ThriftClientBuilder.newBuilder(DummyService.Iface.class)
                        .withZookeeper(ZK).withServiceName("serviceA").withMultiplexer("gateway").build();
                assertFalse(((LitelinksServiceClient) clientAprox).awaitAvailable(1500));

                // start proxy service
                System.setProperty(MultiplexingProxyService.TARGET_ZK_EV, ZK);
                final String configPath = getClass().getResource("/proxiedServices.config").getFile();
                System.setProperty(MultiplexingProxyService.SVC_LIST_FILE_ARG, configPath);
                gway = LitelinksService.createService(MultiplexingProxyService.class, ZK, "gateway").startAsync();
                // check first proxy client now working
                assertTrue(((LitelinksServiceClient) clientAprox).awaitAvailable(5000));
                assertEquals("serviceA-response", clientAprox.method_one(null, null, false));
                // create & test proxied clients for other services
                DummyService.Iface clientBprox = ThriftClientBuilder.newBuilder(DummyService.Iface.class)
                        .withZookeeper(ZK).withServiceName("serviceB").withMultiplexer("gateway").build();
                DummyService2.Iface clientCprox = ThriftClientBuilder.newBuilder(DummyService2.Iface.class)
                        .withZookeeper(ZK).withServiceName("serviceC").withMultiplexer("gateway").build();
                assertEquals("testonly", clientCprox.just_one_method(null));
                assertEquals("serviceB-response", clientBprox.method_one(null, null, false));

                // now stop one of the services
                sA.stopAsync().awaitTerminated();
                try {
                    // this should fail
                    clientAprox.method_one(null, null, false);
                    fail("Stopped back-end service still responding somehow");
                } catch (Exception e) {
                    // won't actually be any nested SUE, but the message should come through
                    assertTrue(e.getMessage() != null &&
                               e.getMessage().contains(ServiceUnavailableException.class.getSimpleName()));
                }
                // stop proxy service
                gway.stopAsync().awaitTerminated();
                assertFalse(((LitelinksServiceClient) clientBprox).isAvailable());
                assertTrue(((LitelinksServiceClient) clientC).isAvailable());

            } finally {
                LitelinksTests.stopAll(sA, sB, sC, gway);
            }
        }
    }

}
