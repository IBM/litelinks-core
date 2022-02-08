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

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.Service;
import com.ibm.watson.litelinks.InvalidThriftClassException;
import com.ibm.watson.litelinks.LitelinksSystemPropNames;
import com.ibm.watson.litelinks.MethodInfo;
import com.ibm.watson.litelinks.SSLHelper.SSLParams;
import com.ibm.watson.litelinks.ThreadContext;
import com.ibm.watson.litelinks.WTTransportException;
import com.ibm.watson.litelinks.client.AvailabilityListener;
import com.ibm.watson.litelinks.client.ClientClosedException;
import com.ibm.watson.litelinks.client.CustomLoadBalancer;
import com.ibm.watson.litelinks.client.LitelinksServiceClient;
import com.ibm.watson.litelinks.client.LitelinksServiceClient.ServiceInstanceInfo;
import com.ibm.watson.litelinks.client.LoadBalancer;
import com.ibm.watson.litelinks.client.LoadBalancingPolicy;
import com.ibm.watson.litelinks.client.ServiceUnavailableException;
import com.ibm.watson.litelinks.client.TServiceClientManager;
import com.ibm.watson.litelinks.client.ThriftClientBuilder;
import com.ibm.watson.litelinks.server.ConfiguredService;
import com.ibm.watson.litelinks.server.ConfiguredService.ConfigMismatchException;
import com.ibm.watson.litelinks.server.DefaultThriftServer;
import com.ibm.watson.litelinks.server.DefaultThriftServer.SSLMode;
import com.ibm.watson.litelinks.server.Idempotent;
import com.ibm.watson.litelinks.server.InstanceFailures;
import com.ibm.watson.litelinks.server.LitelinksService;
import com.ibm.watson.litelinks.server.LitelinksService.ServiceDeploymentConfig;
import com.ibm.watson.litelinks.server.ReleaseAfterResponse;
import com.ibm.watson.litelinks.test.thrift.DummyEnum;
import com.ibm.watson.litelinks.test.thrift.DummyService;
import com.ibm.watson.litelinks.test.thrift.DummyService2;
import com.ibm.watson.litelinks.test.thrift.DummyStruct;
import com.ibm.watson.litelinks.test.thrift.InstanceFailingException;
import com.ibm.watson.litelinks.test.thrift.TestException;
import com.ibm.watson.zk.ZookeeperClient;
import io.netty.util.internal.ThreadLocalRandom;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingServer;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.transport.TTransportException;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.MDC;

import javax.net.ssl.SSLSession;
import javax.security.cert.X509Certificate;
import java.io.Closeable;
import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.ibm.watson.litelinks.server.ZookeeperWatchedService.getServicePath;
import static org.junit.Assert.*;

@SuppressWarnings("unchecked")
public class LitelinksTests {

    /*
     * Tests still TODO:
     *
     * - Per-method timeouts, client fallbacks
     * - Startup, shutdown failures
     * - (more) Clean svc instance shutdown (quiescing reqs in flight)
     * - (more) error handling
     *
     */

    public static String ZK; // = System.getenv("ZOOKEEPER");

    static {
        System.setProperty("litelinks.threadcontexts", "custom,log4j_mdc");
        System.setProperty("litelinks.delay_client_close", "false");

        //System.setProperty("litelinks.serialize_async_callbacks", "true");
    }

    private static TestingServer localZk;
    private static Service testService, testService2, sslService, sslCAService;

    private static final DummyService.Iface defaultImpl = new TestThriftServiceImpl();

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        if (ZK == null || "".equals(ZK.trim())) {
            localZk = new TestingServer();
            ZK = localZk.getConnectString();
        }
        System.out.println("Using ZK connstring: " + ZK);

        // using same store for key and trust for now
        // contains a self-signed cert
        final String keystorePath = LitelinksTests.class.getResource("/keystore.jks").getFile();
        System.setProperty("litelinks.ssl.keystore.path", keystorePath);
        System.setProperty("litelinks.ssl.keystore.password", "password");
        System.setProperty("litelinks.ssl.truststore.path", keystorePath);
        System.setProperty("litelinks.ssl.truststore.password", "password");
        SSLParams.resetDefaultParameters();

        ChangeableThriftServiceImpl.delegate = defaultImpl;
        testService = LitelinksService.createService(ChangeableThriftServiceImpl.class,
                ZK, null).startAsync();
        sslService = LitelinksService.createService(TestThriftServiceImpl.class,
                ZK, "dummysslservice", -1, null, SSLMode.ENABLED).startAsync();
        sslCAService = LitelinksService.createService(TestThriftServiceImpl.class,
                ZK, "dummysslcaservice", -1, null, SSLMode.CLIENT_AUTH).startAsync();
        (testService2 = LitelinksService.createService(TestThriftServiceImpl.class,
                ZK, "dummyservice2")).startAsync();
        testService.awaitRunning();
//		Thread.sleep(2000); // sleep no longer needed since we make use of use client.awaitAvailable() method
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        // stop test services
        stopAll(testService, sslService, sslCAService, testService2);

        // disconnect ZK clients before stopping ZK
        ZookeeperClient.shutdown(false, false);
        Thread.sleep(3000);
        // stop ZK
        if (localZk != null) {
            localZk.close();
        }
    }

    @Test
    public void methodinfo_tests() throws Exception {
        synchronized (SettableThriftServiceImpl.class) {
            try {
                SettableThriftServiceImpl.initPause = 0;
                SettableThriftServiceImpl.shutdownPause = 0;
                SettableThriftServiceImpl.template = new SimpleThriftServiceImpl();
                SettableThriftServiceImpl.mInfos = ImmutableMap.of("nonexist_method", MethodInfo.IDEMPOTENT,
                        "method_one", MethodInfo.IDEMPOTENT);
                Service svc = LitelinksService.createService(SettableThriftServiceImpl.class, ZK, "invalidmitest")
                        .startAsync();
                svc.awaitRunning();
                fail("invalid methinfo should throw");
                stopAll(svc);
            } catch (IllegalStateException e) {
                System.out.println("exception: " + e.getCause());
                assertTrue(e.getCause().getMessage().contains("don't belong to the service interface"));
                assertTrue(e.getCause().getMessage().contains("nonexist_method"));
            } finally {
                SettableThriftServiceImpl.mInfos = null;
            }

            Set<Class<? extends Exception>> exSet =
                    Collections.singleton(InstanceFailingException.class);
            Map<String, MethodInfo> minfos1 = ImmutableMap.of("method_one", MethodInfo.builder().setIdempotent(false)
                    .setInstanceFailureExceptions(exSet).build(), "method_two", MethodInfo.builder()
                    .setInstanceFailureExceptions(exSet).setIdempotent(true).build());
            Map<String, MethodInfo> minfos2 = ImmutableMap.of("method_one", MethodInfo.builder().setIdempotent(false)
                    .setInstanceFailureExceptions(exSet).build(), MethodInfo.DEFAULT, MethodInfo.builder()
                    .setInstanceFailureExceptions(exSet).setIdempotent(true).build());

            DummyService.Iface regularTemplate = new SimpleThriftServiceImpl();
            DummyService.Iface template1throw = new DummyService.Iface() {
                @Override
                public DummyStruct method_two(int arg1, String arg2, ByteBuffer arg3)
                        throws TException {
                    throw new InstanceFailingException("oh poo");
                }

                @Override
                public String method_one(String arg1, DummyStruct arg2, boolean arg3)
                        throws TException {
                    throw new InstanceFailingException("oh poo2");
                }
            };
            // test with annots
            DummyService.Iface templateAnnotThrow = new DummyService.Iface() {
                @Idempotent
                @InstanceFailures(InstanceFailingException.class)
                @Override
                public DummyStruct method_two(int arg1, String arg2, ByteBuffer arg3)
                        throws TException {
                    throw new InstanceFailingException("oh poo");
                }

                @InstanceFailures(InstanceFailingException.class)
                @Override
                public String method_one(String arg1, DummyStruct arg2, boolean arg3)
                        throws TException {
                    throw new InstanceFailingException("oh poo2");
                }
            };

            DummyService.Iface[] templateTests =
                    { template1throw, templateAnnotThrow, template1throw };
            Map<String, MethodInfo>[] minfoTests = new Map[] { minfos1, null, minfos2 };

            System.setProperty(LitelinksSystemPropNames.INSTANCE_FAIL_DELAY_SECS, "2");
            String svcName = "minfotest1";

            DummyService.Iface client = ThriftClientBuilder.newBuilder(DummyService.Iface.class)
                    .withZookeeper(ZK).withServiceName(svcName).build();
            try (LitelinksServiceClient llsc = (LitelinksServiceClient) client) {

                Service svc1 = null, svc2 = null;

                for (int t = 0; t < templateTests.length; t++) {
                    System.out.println("starting methinfo test " + t);
                    assertEquals(0, countState(llsc, true));
                    assertEquals(0, countState(llsc, false));
                    SettableThriftServiceImpl.mInfos = minfoTests[t];
                    try {
                        SettableThriftServiceImpl.template = regularTemplate;
                        svc1 = LitelinksService.createService(SettableThriftServiceImpl.class, ZK, svcName)
                                .startAsync();
                        svc1.awaitRunning();
                        SettableThriftServiceImpl.template = templateTests[t];
                        svc2 = LitelinksService.createService(SettableThriftServiceImpl.class, ZK, svcName)
                                .startAsync();
                        svc2.awaitRunning();
                        Thread.sleep(500L);

                        assertEquals(2, countState(llsc, true));
                        boolean fail = false;
                        for (int i = 0; i < 4; i++) {
                            try {
                                client.method_one("ba", null, true);
                            } catch (InstanceFailingException e) {
                                assertFalse("should only see one failure", fail);
                                fail = true;
                            }
                        }
                        assertTrue("should have had a failure", fail);
                        assertEquals(1, countState(llsc, true));
                        assertEquals(1, countState(llsc, false));
                        System.out.println("sleeping for 2.2sec");
                        Thread.sleep(2200L); // sleep for fail delay to expire
                        assertEquals(2, countState(llsc, true));
                        assertEquals(0, countState(llsc, false));
                        // should not get any failures - because of idempotence
                        for (int i = 0; i < 4; i++) {
                            client.method_two(7, "ho", ByteBuffer.allocate(0));
                        }
                        // shoudl still be one in failed state
                        assertEquals(1, countState(llsc, true));
                        assertEquals(1, countState(llsc, false));

                    } finally {
                        stopAll(svc1, svc2);
                    }
                }
            } finally {
                SettableThriftServiceImpl.template = null;
                SettableThriftServiceImpl.mInfos = null;
            }
        }
    }

    private static int countState(LitelinksServiceClient client, boolean active) {
        int count = 0;
        for (ServiceInstanceInfo sii : client.getServiceInstanceInfo()) {
            if (sii.isActive() == active) {
                count++;
            }
        }
        return count;
    }

    @After
    public void releaseRequestThreadBufs() {
        ReleaseAfterResponse.releaseAll();
    }

    @Test
    public void invalid_version_or_instid_test() throws Exception {
        String[] invalids = { "no\nlinebreak" };
        String[] valids = { "contains=equals   andspaces" };
        Service svc = null;
        for (String invalid : invalids) {
            try {
                (svc = LitelinksService.createService(new ServiceDeploymentConfig("invalid_test1")
                        .setServiceClass(TestThriftServiceImpl.class).setZkConnString(ZK)
                        .setServiceVersion(invalid)).startAsync()).awaitRunning(4, TimeUnit.SECONDS);
                stopAll(svc);
                fail("invalid version string should have failed");
            } catch (IllegalStateException iae) {
                assertTrue(iae.getCause().getMessage().contains("can't contain line break"));
            }
            try {
                (svc = LitelinksService.createService(new ServiceDeploymentConfig("invalid_test1")
                        .setServiceClass(TestThriftServiceImpl.class).setZkConnString(ZK)
                        .setInstanceId(invalid)).startAsync()).awaitRunning(4, TimeUnit.SECONDS);
                stopAll(svc);
                fail("invalid version string should have failed");
            } catch (IllegalStateException iae) {
                assertTrue(iae.getCause().getMessage().contains("can't contain line break"));
            }
        }
        LitelinksServiceClient client = ThriftClientBuilder.newBuilder()
                .withZookeeper(ZK).withServiceName("valid_test1").build();
        try {
            for (String valid : valids) {
                try {
                    svc = LitelinksService.createService(new ServiceDeploymentConfig("valid_test1")
                            .setServiceClass(TestThriftServiceImpl.class).setZkConnString(ZK)
                            .setServiceVersion(valid)).startAsync();
                    assertTrue(client.awaitAvailable(4000L));
                    assertEquals(valid, client.getServiceInstanceInfo().get(0).getVersion());
                    // also ensure metadata is empty map
                    assertEquals(Collections.emptyMap(), client.getServiceInstanceInfo().get(0).getMetadata());
                } finally {
                    stopAll(svc);
                }
                try {
                    svc = LitelinksService.createService(new ServiceDeploymentConfig("valid_test1")
                            .setServiceClass(TestThriftServiceImpl.class).setZkConnString(ZK)
                            .setInstanceId(valid)).startAsync();
                    assertTrue(client.awaitAvailable(4000L));
                    assertEquals(valid, client.getServiceInstanceInfo().get(0).getInstanceId());
                } finally {
                    stopAll(svc);
                }
            }
        } finally {
            client.close();
        }
    }

    @Test
    public void custom_metadata_test() throws Exception {
        Map<String, String> myMetadata = ImmutableMap.of("key1", "val1",
                "  ==\n (nast.yKey)", "  & ==\n (nastyVal)", "app.app..", "", "", "  ");
        Service svc = null;
        try {
            synchronized (SettableThriftServiceImpl.class) {
                SettableThriftServiceImpl.initPause = 0;
                SettableThriftServiceImpl.shutdownPause = 0;
                SettableThriftServiceImpl.template = new SimpleThriftServiceImpl();
                SettableThriftServiceImpl.customParams = myMetadata;
                svc = LitelinksService.createService(SettableThriftServiceImpl.class, ZK, "metadatatest").startAsync();
                svc.awaitRunning();
                SettableThriftServiceImpl.customParams = null;
            }
            final DummyService.Iface client = ThriftClientBuilder.newBuilder(DummyService.Client.class)
                    .withZookeeper(ZK).withServiceName("metadatatest").buildOnceAvailable(3000L);
            // abstract map equality
            assertEquals(myMetadata, ((LitelinksServiceClient) client).getServiceInstanceInfo().get(0).getMetadata());
            assertNotSame(myMetadata, ((LitelinksServiceClient) client).getServiceInstanceInfo().get(0).getMetadata());
            svc.stopAsync().awaitTerminated();
        } finally {
            stopAll(svc);
        }
    }

    @Test
    public void sdi_and_instance_id_test() throws Exception {
        Service svc = null;
        DummyService.Iface client = null;
        String name = "sditest-one", instId = "test-inst-id", vers = "test-vers";
        try {
            synchronized (SDITestingThriftServiceImpl.class) {
                SDITestingThriftServiceImpl.provideName = false;
                SDITestingThriftServiceImpl.provideVersion = false;
                svc = LitelinksService.createService(new ServiceDeploymentConfig(name)
                        .setServiceClass(SDITestingThriftServiceImpl.class).setZkConnString(ZK)
                        .setServiceVersion(vers).setInstanceId(instId)).startAsync();
                svc.awaitRunning();
            }
            client = ThriftClientBuilder.newBuilder(DummyService.Client.class)
                    .withZookeeper(ZK).withServiceName(name).buildOnceAvailable(5000l);
            assertEquals(instId,
                    ((LitelinksServiceClient) client)
                            .getServiceInstanceInfo().get(0).getInstanceId());
            assertEquals(vers,
                    ((LitelinksServiceClient) client)
                            .getServiceInstanceInfo().get(0).getVersion());
            String rtn = client.method_one("na", null, true);
            System.out.println("SDI test returned: " + rtn);
            String[] parts = rtn.split("\\|");
            assertEquals(3, parts.length);
            assertEquals(name + "__" + vers + "__" + instId, parts[0]);
            String[] rtvals = parts[2].split("__");
            int pubport = Integer.parseInt(rtvals[0]),
                    lport = Integer.parseInt(rtvals[4]);
            assertTrue(pubport > 0);
            assertEquals(((LitelinksServiceClient) client)
                    .getServiceInstanceInfo().get(0).getPort(), pubport);
            assertEquals(pubport, lport);
            assertEquals(name, rtvals[1]);
            assertEquals(vers, rtvals[2]);
            assertEquals(InetSocketAddress.class.getName(), rtvals[3]);
        } finally {
            if (client != null) {
                ((LitelinksServiceClient) client).close();
            }
            stopAll(svc);
        }
    }

    @Test
    public void impl_provided_name_and_vers_test() throws Exception {
        Service svc = null;
        DummyService.Iface client = null;
        try {
            synchronized (SDITestingThriftServiceImpl.class) {
                SDITestingThriftServiceImpl.provideName = true;
                SDITestingThriftServiceImpl.provideVersion = true;
                // don't provide service name or version externally
                svc = LitelinksService.createService(new ServiceDeploymentConfig()
                                .setServiceClass(SDITestingThriftServiceImpl.class).setZkConnString(ZK))
                        .startAsync();
                svc.awaitRunning();
            }
            client = ThriftClientBuilder.newBuilder(DummyService.Client.class)
                    .withZookeeper(ZK).withServiceName("impl-provided-name").buildOnceAvailable(5000l);
            assertEquals("impl-provided-vers",
                    ((LitelinksServiceClient) client)
                            .getServiceInstanceInfo().get(0).getVersion());
            String instId = ((LitelinksServiceClient) client)
                    .getServiceInstanceInfo().get(0).getInstanceId();
            // check there's a reasonable default instanceid
            System.out.println("instance id is " + instId);
            assertNotNull(instId);
            assertTrue(instId.length() > 0);
            assertFalse(instId.matches(".*\\s.*")); // no whitespace

            String rtn = client.method_one("na", null, true);
            System.out.println("SDI test2 returned: " + rtn);
            String[] parts = rtn.split("\\|");
            assertEquals(3, parts.length);
            assertTrue(parts[0].endsWith("__" + instId));
            String[] rtvals = parts[2].split("__");
            assertEquals("impl-provided-name", rtvals[1]);
            assertEquals("impl-provided-vers", rtvals[2]);
            assertEquals(InetSocketAddress.class.getName(), rtvals[3]);
        } finally {
            if (client != null) {
                ((LitelinksServiceClient) client).close();
            }
            stopAll(svc);
        }
    }

    @Test
    public void request_listener_test() throws Exception {
        String name = "rltest", instId = "insto";
        Service svc = null;
        try {
            TestRequestListener impl;
            DummyService.Iface client;
            synchronized (TestRequestListener.class) {
                TestRequestListener.initExceptionMsg = null;
                svc = LitelinksService.createService(new ServiceDeploymentConfig(SimpleThriftServiceImpl.class)
                                .setZkConnString(ZK).setServiceName(name).setInstanceId(instId).setStartTimeoutSecs(5)
                                .setReqListenerClasses(Collections.
                                        singletonList(TestRequestListener.class)))
                        .startAsync();

                client = ThriftClientBuilder.newBuilder(DummyService.Client.class)
                        .withZookeeper(ZK).withServiceName(name).withTimeout(5000).buildOnceAvailable(5000l);
                impl = TestRequestListener.lastInst;
            }
            ThreadContext.removeCurrentContext();
            assertEquals(name, impl.iName);
            assertEquals(instId, impl.iInstId);
            try (LitelinksServiceClient lsc = (LitelinksServiceClient) client) {
                String key = "param", val = "testvalue";
                ThreadContext.setCurrentContext(Collections.singletonMap(key, val));
                basic_test(client, false);
                assertEquals(val, impl.nParam);
                assertEquals("method_two", impl.nMethod);
                assertTrue(impl.nRemoteAddress instanceof InetSocketAddress);
                assertNull(impl.nSslSession); // null because ssl not enabled
                Thread.sleep(500l); // reqComplete meth might still be in progress
                assertEquals(val, impl.cParam);
                assertEquals("method_two", impl.cMethod);
                assertTrue(impl.cRemoteAddress instanceof InetSocketAddress);
                // test request rejection (RL exceptions)
                ThreadContext.removeCurrentContext();
                String exMsg = "Auth Exception!";
                impl.thisNewReqExceptionMsg = exMsg;
                try {
                    basic_test(client);
                    fail("Client invocation should have thrown exception"
                            + " from req listener");
                } catch (Exception e) {
                    if (!(e instanceof TApplicationException)) {
                        System.err.println("Unexpected exception type: " + e.getClass().getName());
                        throw e;
                    }
                    assertNotNull(e.getMessage());
                    assertTrue(e.getMessage().contains(exMsg));
                }
                assertNull(impl.nParam);
                assertEquals(val, impl.cParam);
                // check client still works
                impl.thisNewReqExceptionMsg = null;
                basic_test(client, false);
                Thread.sleep(500l); // reqComplete meth might still be in progress
                assertNull(impl.cParam);
            }
            svc.stopAsync().awaitTerminated();
            // test RL aborting service init (via initialize method exception)
            String exMsg = "init exception!";
            synchronized (TestRequestListener.class) {
                TestRequestListener.initExceptionMsg = exMsg;
                svc = LitelinksService.createService(new ServiceDeploymentConfig(SimpleThriftServiceImpl.class)
                                .setZkConnString(ZK).setServiceName(name).setInstanceId(instId).setStartTimeoutSecs(5)
                                .setReqListenerClasses(Collections.singletonList(TestRequestListener.class)))
                        .startAsync();
                try {
                    svc.awaitRunning();
                    fail("service init didn't fail");
                } catch (IllegalStateException ise) {
                    assertNotNull(ise.getCause());
                    assertNotNull(ise.getCause().getMessage());
                    assertTrue(ise.getCause().getMessage().contains(exMsg));
                }
            }
            assertEquals(Service.State.FAILED, svc.state());
            svc = null;
        } finally {
            stopAll(svc);
        }
    }

    @Test
    public void request_listener_sslsess_test() throws Exception {
        String name = "rltestssl", instId = "insto2";
        Service svc = null;
        try {
            TestRequestListener impl;
            DummyService.Iface client;
            synchronized (TestRequestListener.class) {
                TestRequestListener.initExceptionMsg = null;
                svc = LitelinksService.createService(new ServiceDeploymentConfig(SimpleThriftServiceImpl.class)
                                .setZkConnString(ZK).setServiceName(name).setInstanceId(instId).setStartTimeoutSecs(5)
                                .setSslMode(SSLMode.CLIENT_AUTH)
                                .setReqListenerClasses(Collections.singletonList(TestRequestListener.class)))
                        .startAsync();

                client = ThriftClientBuilder.newBuilder(DummyService.Client.class)
                        .withZookeeper(ZK).withServiceName(name).withTimeout(5000).buildOnceAvailable(5000l);
                impl = TestRequestListener.lastInst;
            }
            ThreadContext.removeCurrentContext();
            assertEquals(name, impl.iName);
            assertEquals(instId, impl.iInstId);
            try (LitelinksServiceClient lsc = (LitelinksServiceClient) client) {
                basic_test(client, false);
                assertTrue(impl.nSslSession instanceof SSLSession);
                SSLSession cxt = (SSLSession) impl.nSslSession;
                X509Certificate[] certs = cxt.getPeerCertificateChain();
                System.out.println("Client DN is: " + certs[0].getSubjectDN().getName());
            }
        } finally {
            stopAll(svc);
        }
    }

    @Test
    public void init_timeout_test() throws Exception {
        Service svc = null;
        try {
            synchronized (SettableThriftServiceImpl.class) {
                SettableThriftServiceImpl.initPause = 1000 * 60;
                svc = LitelinksService.createService(new ServiceDeploymentConfig(SettableThriftServiceImpl.class)
                        .setZkConnString(ZK).setServiceName("timeouttest").setStartTimeoutSecs(5)).startAsync();
                long t = System.currentTimeMillis();
                try {
                    svc.awaitRunning();
                    fail("did not timeout");
                } catch (IllegalStateException ise) {
                    long took = System.currentTimeMillis() - t;
                    assertTrue("non-timeout failure",
                            ise.getCause() != null && ise.getCause() instanceof TimeoutException);
                    assertNotNull("hung thread stacktrace not included", ise.getCause().getCause());
                    assertTrue("5sec timeout took " + took, took > 4500 && took < 5500);
                }
            }
        } finally {
            if (svc.state() != Service.State.FAILED) {
                stopAll(svc);
            }
        }
    }

    @Test
    public void shutdown_timeout_test() throws Exception {
        Service svc = null;
        ExecutorService es = Executors.newSingleThreadExecutor();
        try {
            synchronized (SettableThriftServiceImpl.class) {
                SettableThriftServiceImpl.initPause = 0;
                SettableThriftServiceImpl.shutdownPause = 6000;
                SettableThriftServiceImpl.template = new DummyService.Iface() {
                    @Override
                    public DummyStruct method_two(int arg1, String arg2, ByteBuffer arg3)
                            throws TException {return null;}

                    @Override
                    public String method_one(String arg1, DummyStruct arg2, boolean arg3)
                            throws TException {
                        try {
                            Thread.sleep(60000l);
                        } catch (InterruptedException e) {
                            throw new TException(e);
                        }
                        return arg1;
                    }
                };
                svc = LitelinksService.createService(SettableThriftServiceImpl.class, ZK, "hangmethodtest")
                        .startAsync();
                svc.awaitRunning();
            }

            final DummyService.Iface client = ThriftClientBuilder.newBuilder(DummyService.Client.class)
                    .withZookeeper(ZK).withServiceName("hangmethodtest").withTimeout(300000).buildOnceAvailable(5000l);
            // call method in separate thread, then try shutdown
            es.submit(new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    return client.method_one("test", null, false);
                }
            });

            long t = System.currentTimeMillis();
            svc.stopAsync();
            // stop should complete within 41sec
            svc.awaitTerminated(41, TimeUnit.SECONDS);
            long took = System.currentTimeMillis() - t;
            assertTrue("service stopped too quickly (" + took + " ms)", took > 33 * 1000);
        } finally {
            es.shutdownNow();
            stopAll(svc);
        }
    }


    @Test
    public void anon_client_test() throws Exception {
        LitelinksServiceClient client = ThriftClientBuilder.newBuilder()
                .withServiceName(DummyService.class.getName())
                .withZookeeper(ZK).withTimeout(5000).buildOnceAvailable(5000l);

        testConn(client);

        List<ServiceInstanceInfo> sii = client.getServiceInstanceInfo();
        System.out.println(sii);
        System.out.println(sii.get(0).getPort());
        assertTrue(sii.get(0).isActive());
    }

    @Test
    public void thread_context_test() throws Exception {
        Service svc = null;
        try {
            synchronized (SettableThriftServiceImpl.class) {
                SettableThriftServiceImpl.initPause = 0;
                SettableThriftServiceImpl.template = new DummyService.Iface() {
                    @Override
                    public DummyStruct method_two(int arg1, String arg2, ByteBuffer arg3)
                            throws TException {return null;}

                    @Override
                    public String method_one(String arg1, DummyStruct arg2, boolean arg3)
                            throws TException {
                        Map<String, String> cxt = ThreadContext.getCurrentContext();
                        if (cxt == null) {
                            return "";
                        }
                        String v1 = cxt.get(arg1);
                        return v1 + "____" + cxt.size();
                    }
                };
                svc = LitelinksService.createService(SettableThriftServiceImpl.class, ZK, "cxttest").startAsync();
                svc.awaitRunning();
            }

            ThreadContext.removeCurrentContext();
            DummyService.Iface client = ThriftClientBuilder.newBuilder(DummyService.Client.class)
                    .withZookeeper(ZK).withServiceName("cxttest")
                    .withTimeout(5, TimeUnit.MINUTES).buildOnceAvailable(5, TimeUnit.SECONDS);

            assertEquals("", client.method_one("key", null, true));
            ThreadContext.setCurrentContext(Collections.singletonMap("key", "blaaa"));
            assertEquals("blaaa____1", client.method_one("key", null, true));
            assertEquals("blaaa____1", client.method_one("key", null, true));
            Map<String, String> cxt2 = new HashMap<>();

            cxt2.put("key0", "KEY0VAL!");
            cxt2.put("key3", "vaaal3");
            ThreadContext.setCurrentContext(cxt2);
            assertEquals("null____2", client.method_one("key2", null, true));
            assertEquals("vaaal3____2", client.method_one("key3", null, true));
            cxt2.put("key2", "newval");
            assertEquals("newval____3", client.method_one("key2", null, true));
            cxt2.put("key2", null);
            assertEquals("null____3", client.method_one("key2", null, true));
            ThreadContext.removeCurrentContext();
            assertEquals("", client.method_one("key2", null, true));

            ((Closeable) client).close();

            cxt2.remove("key2");
            client = ThriftClientBuilder.newBuilder(DummyService.Client.class)
                    .withZookeeper(ZK).withServiceName("cxttest")
                    .withContext(cxt2).withContextEntry("key4", "default_k4val")
                    .withTimeout(5, TimeUnit.MINUTES).buildOnceAvailable(5, TimeUnit.SECONDS);
            assertEquals("KEY0VAL!____3", client.method_one("key0", null, true));
            Map<String, String> newmap = new HashMap<>();
            newmap.put("newkey1", "someval");
            ThreadContext.setCurrentContext(newmap);
            assertEquals("someval____4", client.method_one("newkey1", null, true));
            assertEquals("KEY0VAL!____4", client.method_one("key0", null, true));
            ThreadContext.addContextEntry("newkey2", "bacon");
            assertEquals("someval____5", client.method_one("newkey1", null, true));
            assertEquals("KEY0VAL!____5", client.method_one("key0", null, true));
            assertEquals("bacon____5", client.method_one("newkey2", null, true));
            ThreadContext.addContextEntry("key0", "override");
            assertEquals("override____5", client.method_one("key0", null, true));
            assertEquals("vaaal3____5", client.method_one("key3", null, true));
            assertEquals("default_k4val____5", client.method_one("key4", null, true));

            // test context proxy
            long b4 = System.nanoTime();
            DummyService.Iface ctxProx = ((LitelinksServiceClient) client)
                    .contextProxy(Collections.singletonMap("key3", "proxval"));
            System.out.println("ctxProx create took " + (System.nanoTime() - b4) / 1000 + "microsecs");
            b4 = System.nanoTime();
            DummyService.Iface ctxProx2 = ((LitelinksServiceClient) client)
                    .contextProxy(Collections.singletonMap("newprxkey", "otherproxval"));
            System.out.println("ctxProx2 create took " + (System.nanoTime() - b4) / 1000 + "microsecs");
            b4 = System.nanoTime();
            assertEquals("proxval____5", ctxProx.method_one("key3", null, true));
            System.out.println("prox method call took " + (System.nanoTime() - b4) / 1000 + "microsecs");
            b4 = System.nanoTime();
            assertEquals("vaaal3____5", client.method_one("key3", null, true));
            System.out.println("regular method call took " + (System.nanoTime() - b4) / 1000 + "microsecs");
            assertEquals("otherproxval____6", ctxProx2.method_one("newprxkey", null, true));
        } finally {
            ThreadContext.removeCurrentContext();
            stopAll(svc);
        }
    }

    @Test
    public void mdc_test() throws Exception {
        Service svc = null;
        try {
            String sname = "mdctest";
            synchronized (SettableThriftServiceImpl.class) {
                SettableThriftServiceImpl.initPause = 0;
                SettableThriftServiceImpl.template = new DummyService.Iface() {
                    @Override
                    public DummyStruct method_two(int arg1, String arg2, ByteBuffer arg3)
                            throws TException {return null;}

                    @Override
                    public String method_one(String arg1, DummyStruct arg2, boolean arg3)
                            throws TException {
                        Map<String, String> cxt = ThreadContext.getCurrentContext();
                        String v1 = MDC.get(arg1), v2 = cxt != null ? cxt.get(arg1) : "";
                        Map<String, String> mdcMap = MDC.getCopyOfContextMap();
                        return v1 + "____" + (mdcMap != null ? mdcMap.size() : "null") + (cxt != null ? "__" + v2 : "");
                    }
                };
                svc = LitelinksService.createService(SettableThriftServiceImpl.class, ZK, sname).startAsync();
                svc.awaitRunning();
            }

            ThreadContext.removeCurrentContext();

            DummyService.Iface client = ThriftClientBuilder.newBuilder(DummyService.Client.class)
                    .withZookeeper(ZK).withServiceName(sname).withTimeout(300000).buildOnceAvailable(5000l);

            assertEquals("null____0", client.method_one("key", null, true));
            MDC.put("key", "blaaa");
            assertEquals("blaaa____1", client.method_one("key", null, true));
            assertEquals("blaaa____1", client.method_one("key", null, true));
            MDC.clear();
            ThreadContext.setCurrentContext(Collections.singletonMap("key3", "testboth"));
            MDC.put("key3", "3");
            assertEquals("null____1__null", client.method_one("key2", null, true));
            assertEquals("3____1__testboth", client.method_one("key3", null, true));
            MDC.put("key2", "newval");
            ThreadContext.removeCurrentContext();
            assertEquals("newval____2", client.method_one("key2", null, true));
            MDC.clear();
            assertTrue(Arrays.asList("null____0", "null____null").contains(client.method_one("key2", null, true)));
        } finally {
            stopAll(svc);
        }
    }

    @Test
    public void conn_failure_tests() throws Exception {
        /*
         * This verifies that failover/retries are happening on connection errors
         * by making sure there are no observed errs on client side,
         * and that server-side invocation counts match up
         */
        int port = 1088, dummy = 1087;
        final AtomicInteger count1 = new AtomicInteger(), count2 = new AtomicInteger();
        Service svcA = null, svcB = null;
        String sname = "failtest";
        synchronized (SettableThriftServiceImpl.class) {
            try {
                startReal(port, count1);
                SettableThriftServiceImpl.initPause = 0;
                SettableThriftServiceImpl.template = new DummyService.Iface() {
                    @Override
                    public DummyStruct method_two(int arg1, String arg2, ByteBuffer arg3)
                            throws TException {return null;}

                    @Override
                    public String method_one(String arg1, DummyStruct arg2, boolean arg3)
                            throws TException {
                        try {
                            Thread.sleep((long) (200 * Math.random()));
                        } catch (InterruptedException e) {
                            throw new TException(e);
                        }
                        return Integer.toString(count2.incrementAndGet());
                    }
                };
                svcA = LitelinksService.createService(new ServiceDeploymentConfig(TestThriftServiceImpl.class)
                        .setZkConnString(ZK).setServiceName(sname).setPort(dummy).setPublicPort(port)).startAsync();
                svcB = LitelinksService.createService(SettableThriftServiceImpl.class,
                        ZK, "failtest").startAsync();
                svcB.awaitRunning();

                DummyService.Iface client = ThriftClientBuilder.newBuilder(DummyService.Iface.class)
                        .withZookeeper(ZK).withServiceName(sname).withTimeout(300000).buildOnceAvailable(5000l);
                for (int i = 0; i < 24; i++) {
                    client.method_one("", null, true);
                }
                int c1 = count1.get(), c2 = count2.get();
                System.out.println("c1 = " + c1 + " c2 = " + c2);
                assertEquals(24, c1 + c2);
                assertTrue(c1 != 0 && c2 != 0);
                realsvc.stopAsync().awaitTerminated();
                for (int i = 0; i < 20; i++) {
                    client.method_one("", null, true); // should be no failures
                }
                assertEquals(c1, count1.get());
                assertEquals(c2 + 20, count2.get());
                c2 = count2.get();
                System.out.println("c1 = " + c1 + " c2 = " + c2);
                startReal(port, count1);
                Thread.sleep(5000); // give 6 secs to detect now alive
                for (int i = 0; i < 20; i++) {
                    client.method_one("", null, true);
                }
                assertTrue(count1.get() > c1);
                c1 = count1.get();
                c2 = count2.get();
                System.out.println("c1 = " + c1 + " c2 = " + c2);
                assertEquals(64, c1 + c2);
                System.out.println("Testing testConnection method");
                for (int i = 0; i < 10; i++) {
                    testConn(client);
                }
                realsvc.stopAsync().awaitTerminated();
                System.out.println("Testing testConnection method 1");
                testConn(client);
                System.out.println("Testing testConnection method 2");
                testConn(client);
                svcB.stopAsync().awaitTerminated();
                assertTrue(((LitelinksServiceClient) client).isAvailable());
                try {
                    System.out.println("Testing testConnection method 3");
                    testConn(client);
                    fail("testconn should fail");
                } catch (WTTransportException e) {
                }
                startReal(port, count1);
                System.out.println("Testing testConnection method 4");
                testConn(client);
                stopAll(svcA, svcB, realsvc);
                Thread.sleep(500);
                try {
                    System.out.println("Testing testConnection method 5");
                    testConn(client);
                    fail("testconn should fail");
                } catch (ServiceUnavailableException e) {
                }
            } finally {
                stopAll(svcA, svcB, realsvc);
            }
        }
    }

    @Test
    public void disable_last_resort_test() throws Exception {
        DummyService.Iface client;
        System.setProperty(LitelinksSystemPropNames.USE_FAILING_INSTANCES, "false");
        try {
            client = ThriftClientBuilder.newBuilder(DummyService.Client.class)
                    // dummy non-existent server
                    .withStaticServer("254.2.3.4", 5678).build();
        } finally {
            System.clearProperty(LitelinksSystemPropNames.USE_FAILING_INSTANCES);
        }
        try {
            basic_test(client);
            fail("test against non-existent server should fail");
        } catch (TException te) {
            te.printStackTrace();
            assertTrue(te.getCause() instanceof ServiceUnavailableException);
            assertTrue(te.getCause().getMessage().startsWith("all instances are failing"));
        }
    }

    @Test
    public void interruption_test() throws Exception {
        /*
         * Client requests should not be retried if interrupted
         */
        final AtomicInteger count1 = new AtomicInteger();
        Service svcA = null;
        String sname = "interrupttest";
        synchronized (SettableThriftServiceImpl.class) {
            ExecutorService es = Executors.newSingleThreadExecutor();
            try {
                SettableThriftServiceImpl.initPause = 0;
                SettableThriftServiceImpl.template = new DummyService.Iface() {
                    @Override
                    public DummyStruct method_two(int arg1, String arg2, ByteBuffer arg3)
                            throws TException {return null;}

                    @Override
                    @Idempotent
                    public String method_one(String arg1, DummyStruct arg2, boolean arg3)
                            throws TException {
                        count1.incrementAndGet();
                        try {
                            Thread.sleep(1000L);
                        } catch (InterruptedException e) {
                            throw new TException(e);
                        }
                        return arg1;
                    }
                };
                svcA = LitelinksService.createService(new ServiceDeploymentConfig(SettableThriftServiceImpl.class)
                        .setZkConnString(ZK).setServiceName(sname)).startAsync();

                final DummyService.Iface client = ThriftClientBuilder.newBuilder(DummyService.Iface.class)
                        .withZookeeper(ZK).withServiceName(sname).withTimeout(20000).buildOnceAvailable(5000l);

                assertEquals(0, count1.get());
                final AtomicReference<Thread> threadref = new AtomicReference<>();
                Future<String> fut = es.submit(new Callable<String>() {
                    @Override
                    public String call() throws Exception {
                        threadref.set(Thread.currentThread());
                        return client.method_one("", null, true);
                    }
                });
                Thread thread;
                while ((thread = threadref.get()) == null) {
                }

                Thread.sleep(200L);
                assertFalse(fut.isDone());
                assertEquals(1, count1.get());
                thread.interrupt();
                Thread.sleep(60);
                assertTrue(fut.isDone());
                try {
                    fut.get();
                    fail("Interrupted exception should have been thrown");
                } catch (ExecutionException ee) {
                    System.out.println("Caught exception chain: " + Throwables.getCausalChain(ee));
                    Throwable cause = ee.getCause();
                    assertTrue(cause instanceof InterruptedException
                            || cause.getCause() instanceof InterruptedException);
                }
                Thread.sleep(200L);
                // ensure method was not retried
                assertEquals(1, count1.get());
            } finally {
                stopAll(svcA);
                es.shutdownNow();
            }
        }
    }

    private static void testConn(Object client) throws Exception {
        long t = System.nanoTime();
        try {
            ((LitelinksServiceClient) client).testConnection();
        } finally {
            System.out.println("testconn took " + (System.nanoTime() - t) / 1000 + "µs");
        }
        // also try explicit instance testconnection
        List<ServiceInstanceInfo> siis = ((LitelinksServiceClient) client).getServiceInstanceInfo();
        if (siis.size() > 0) {
            siis.get(0).testConnection(1000L);
        }

    }

    private Service realsvc;

    private void startReal(int port, final AtomicInteger count) {
        SettableThriftServiceImpl.initPause = 0;
        SettableThriftServiceImpl.template = new DummyService.Iface() {
            @Override
            public DummyStruct method_two(int arg1, String arg2, ByteBuffer arg3) throws TException {return null;}

            @Override
            public String method_one(String arg1, DummyStruct arg2, boolean arg3) throws TException {
                try {
                    Thread.sleep((long) (200 * Math.random()));
                } catch (InterruptedException e) {
                    throw new TException(e);
                }
                return Integer.toString(count.incrementAndGet());
            }
        };
        (realsvc = LitelinksService.createService(SettableThriftServiceImpl.class,
                null, "notused", port, null, null).startAsync()).awaitRunning();
    }

    @Test
    public void method_abort_test() throws Exception {
        method_abort_test(false); // test without property set (default)
        method_abort_test(true); // test with property set
    }

    public void method_abort_test(boolean abort) throws Exception {
        // ensure no cap on num of concurrent conns to a service
        Service svc = null;
        synchronized (SettableThriftServiceImpl.class) {
            try {
                SettableThriftServiceImpl.initPause = 0;
                final CountDownLatch latch = new CountDownLatch(1);
                SettableThriftServiceImpl.template = new DummyService.Iface() {
                    @Override
                    public DummyStruct method_two(int arg1, String arg2, ByteBuffer arg3)
                            throws TException {return null;}

                    @Override
                    public String method_one(String arg1, DummyStruct arg2, boolean arg3)
                            throws TException {
                        try {
                            Thread.sleep(3000L);
                        } catch (InterruptedException e) {
                            throw new TException(e);
                        } finally {
                            latch.countDown();
                        }
                        return "finished!";
                    }
                };
                String threadsBefore = abort ?
                        System.setProperty(LitelinksSystemPropNames.CANCEL_ON_CLIENT_CLOSE, "true") : null;
                try {
                    svc = LitelinksService.createService(SettableThriftServiceImpl.class, ZK, "aborttest").startAsync();
                    svc.awaitRunning();
                } finally {
                    if (abort) {
                        if (threadsBefore == null) {
                            System.clearProperty(LitelinksSystemPropNames.CANCEL_ON_CLIENT_CLOSE);
                        } else {
                            System.setProperty(LitelinksSystemPropNames.CANCEL_ON_CLIENT_CLOSE, threadsBefore);
                        }
                    }
                }
                final DummyService.Iface client = ThriftClientBuilder.newBuilder(DummyService.Iface.class)
                        .withZookeeper(ZK).withServiceName("aborttest").withTimeout(1200).buildOnceAvailable(5000l);
                long before = System.currentTimeMillis();
                try {
                    client.method_one("", null, false);
                    fail("Method should have timed out");
                } catch (TTransportException e) {
                }
                assertTrue(latch.await(4, TimeUnit.SECONDS));
                long took = System.currentTimeMillis() - before;
                System.out.println("took " + took + "ms");
                if (abort) {
                    assertTrue("service thread wasn't interrupted in timely manner", took > 1100 && took < 1400);
                } else {
                    assertTrue("service method didn't take expected 3secs", took > 2900 && took < 3300);
                }
            } finally {
                stopAll(svc);
            }
        }
    }

    @Test
    public void concurrent_conns_test() throws Exception {
        int max = concurrent_conns_test(null, 256);
        assertTrue("Max concurrency was only " + max + " (expecting > 225)", max > 225);
        max = concurrent_conns_test("47", 96);
        assertEquals(47, max);
    }

    public int concurrent_conns_test(String propval, int load) throws Exception {
        // ensure no cap on num of concurrent conns to a service
        Service svc = null;
        ExecutorService exec = Executors.newCachedThreadPool();
        String sname = "conctest";
        synchronized (SettableThriftServiceImpl.class) {
            String propBefore = propval != null ?
                    System.setProperty(LitelinksSystemPropNames.NUM_PROCESS_THREADS, propval) : null;
            try {
                final AtomicInteger inflight = new AtomicInteger(), max = new AtomicInteger();
                SettableThriftServiceImpl.initPause = 0;
                SettableThriftServiceImpl.template = new DummyService.Iface() {
                    @Override
                    public DummyStruct method_two(int arg1, String arg2, ByteBuffer arg3)
                            throws TException {return null;}

                    @Override
                    public String method_one(String arg1, DummyStruct arg2, boolean arg3)
                            throws TException {
                        int num = inflight.incrementAndGet(), curmax = max.get();
                        if (num > curmax) {
                            max.compareAndSet(curmax, num);
                        }
                        try {
                            Thread.sleep(1100l);
                        } catch (InterruptedException e) {
                            throw new TException(e);
                        } finally {
                            inflight.decrementAndGet();
                        }
                        return null;
                    }
                };
                svc = LitelinksService.createService(SettableThriftServiceImpl.class, ZK, sname).startAsync();
                svc.awaitRunning();
                final DummyService.Iface client = ThriftClientBuilder.newBuilder(DummyService.Iface.class)
                        .withZookeeper(ZK).withServiceName(sname).withTimeout(5000).buildOnceAvailable(5000l);
                final CountDownLatch cdl = new CountDownLatch(1);
                Callable<Void> c = new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        cdl.await();
                        client.method_one(null, null, false);
                        return null;
                    }
                };
                for (int i = 0; i < load; i++) {
                    exec.submit(c);
                }
                cdl.countDown();
                exec.shutdown();
                assertTrue("concurrent reqs didn't finish within 10 seconds",
                        exec.awaitTermination(10, TimeUnit.SECONDS));
                int maxconc = max.get();
                System.out.println("Concurrency reached " + maxconc + " out of " + load);
                return maxconc;
            } finally {
                if (propval != null) {
                    if (propBefore == null) {
                        System.clearProperty(LitelinksSystemPropNames.NUM_PROCESS_THREADS);
                    } else {
                        System.setProperty(LitelinksSystemPropNames.NUM_PROCESS_THREADS, propBefore);
                    }
                }
                exec.shutdown();
                stopAll(svc);
            }
        }
    }

    @Test
    public void basic_zk_restart_test() throws Exception {
        Service svc = null;
        ZookeeperClient.SHORT_TIMEOUTS = true;
        final TestingServer tempZk = new TestingServer();
        final String connStr = tempZk.getConnectString(), svcName = "myservice";
        try {
            // note, using "/" appended to the ZK conn string so that a separate ZK conn is used by the
            // client and server
            svc = LitelinksService.createService(ChangeableThriftServiceImpl.class, connStr, svcName).startAsync();
            DummyService.Iface client = ThriftClientBuilder.newBuilder(DummyService.Iface.class)
                    .withZookeeper(connStr + "/").withServiceName(svcName).withTimeout(5000).buildOnceAvailable(5000l);
            System.out.println("first test");
            basic_test(client);

            System.out.println("stopping zk");
            tempZk.stop(); // kill the ZK server
            Thread.sleep(6000); // wait
            System.out.println("test after stopping zk");
            basic_test(client);
            // wait 3 sec (past zk conn timeout)
            Thread.sleep(3000);
            // create new client
            System.out.println("test after waiting past conn timeout");
            basic_test(client);
            // wait 4 more sec (now past zk session timeout)
            Thread.sleep(4000);
            System.out.println("test after waiting past session timeout");
            basic_test(client);
            // now restart zk
            System.out.println("restarting zk");
            tempZk.restart();
            Thread.sleep(2000);
            assertTrue(((LitelinksServiceClient) client).awaitAvailable(3000));
            basic_test(client);

            // shutdown service
            System.out.println("shutting down service");
            svc.stopAsync().awaitTerminated();
            Thread.sleep(4500);

            // client should see that service has gone
            assertFalse(((LitelinksServiceClient) client).isAvailable());
        } finally {
            System.out.println("starting cleanup");
            ZookeeperClient.SHORT_TIMEOUTS = false;
            if (svc != null) {
                svc.stopAsync().awaitTerminated();
            }
            ZookeeperClient.disconnectCurator(connStr);
            ZookeeperClient.disconnectCurator(connStr + "/");
            tempZk.close();
        }
        System.out.println("after");
    }

    @Test
    public void client_waiting_zk_restart_test() throws Exception {
        Service svc = null;
        ZookeeperClient.SHORT_TIMEOUTS = true;
        final TestingServer tempZk = new TestingServer();
        final String connStr = tempZk.getConnectString(), svcName = "myservice2";
        try {
            // note, using "/" appended to the ZK conn string so that a separate ZK conn is used by the
            // client and server
            DummyService.Iface client = ThriftClientBuilder.newBuilder(DummyService.Iface.class)
                    .withZookeeper(connStr + "/").withServiceName(svcName).withTimeout(5000).build();
            assertFalse(((LitelinksServiceClient) client).isAvailable());
            Thread.sleep(1000);
            // now kill ZK
            System.out.println("stopping zk");
            tempZk.stop(); // kill the ZK server & wait for session timeout to elapse
            System.out.println("stopped");
            Thread.sleep(6500);
            long t1 = System.currentTimeMillis();
            assertFalse(((LitelinksServiceClient) client).isAvailable());
            System.out.println("unavailable check took " + (System.currentTimeMillis() - t1) + "ms");
            System.out.println("restarting zk");
            tempZk.restart();
            Thread.sleep(2000);
            System.out.println("starting service");
            svc = LitelinksService.createService(ChangeableThriftServiceImpl.class, connStr, svcName).startAsync();
            // check that client sees it
            boolean ok = ((LitelinksServiceClient) client).awaitAvailable(20000l);
            if (!ok) {
                System.out.println("avail= " + ((LitelinksServiceClient) client).isAvailable()
                        + " children in zk: " + ZookeeperClient.getCurator(tempZk.getConnectString())
                        .getChildren().forPath(getServicePath(svcName)));
            }
            assertTrue(ok);
            basic_test(client);
        } finally {
            System.out.println("starting cleanup");
            ZookeeperClient.SHORT_TIMEOUTS = false;
            if (svc != null) {
                svc.stopAsync().awaitTerminated();
            }
            ZookeeperClient.disconnectCurator(connStr);
            ZookeeperClient.disconnectCurator(connStr + "/");
            tempZk.close();
        }
        System.out.println("after");
    }

    @Test
    public void client_builder_from_iface_test() throws Exception {
        DummyService.Iface client = ThriftClientBuilder.newBuilder(DummyService.Iface.class)
                .withZookeeper(ZK).withTimeout(5000).buildOnceAvailable(5000l);
        basic_test(client);
        // also test building using generated client
        DummyService.Iface client2 = ThriftClientBuilder.newBuilder(DummyService.Client.class)
                .withZookeeper(ZK).withTimeout(5000).buildOnceAvailable(5000l);
        basic_test(client2);
    }

    @Test
    public void simple_svc_class_with_close_test() throws Exception {
        String name = "thriftonlyimpl";
        SimpleThriftServiceImpl.isRunning = false;
        SimpleThriftServiceImpl.WithShutdown.isShutdown = false;
        Service svc = LitelinksService.createService(SimpleThriftServiceImpl.WithShutdown.class,
                ZK, name).startAsync();
        try {
            svc.awaitRunning(5, TimeUnit.SECONDS);
            assertTrue(SimpleThriftServiceImpl.isRunning);
            DummyService.Iface client = ThriftClientBuilder.newBuilder(DummyService.Client.class)
                    .withZookeeper(ZK).withServiceName(name).buildOnceAvailable(3000l);
            basic_test(client, false);
        } finally {
            svc.stopAsync().awaitTerminated();
        }
        assertTrue(SimpleThriftServiceImpl.WithShutdown.isShutdown);
    }

    @Test(expected = InvalidThriftClassException.class)
    public void invalid_svc_class_test() throws Exception {
        LitelinksService.createService(String.class, ZK, "dontmatter");
    }

    @Test
    public void port_map_test() throws Exception {
        int listenPort = 1085, publishPort = 1086;
        Service mappedsvc = null, staticdummysvc = null;
        synchronized (SettableThriftServiceImpl.class) {
            try {
                SettableThriftServiceImpl.template = new DummyService.Iface() {
                    @Override
                    public DummyStruct method_two(int arg1, String arg2, ByteBuffer arg3) throws TException {
                        return null;
                    }

                    @Override
                    public String method_one(String arg1, DummyStruct arg2, boolean arg3)
                            throws TException {
                        return "mapped";
                    }
                };
                mappedsvc = LitelinksService.createService(new ServiceDeploymentConfig(SettableThriftServiceImpl.class)
                        .setZkConnString(ZK).setServiceName("port_mapped_service")
                        .setPort(listenPort).setPublicPort(publishPort));
                mappedsvc.startAsync().awaitRunning(); // needed to ensure correct template is used

                SettableThriftServiceImpl.template = new DummyService.Iface() {
                    @Override
                    public DummyStruct method_two(int arg1, String arg2, ByteBuffer arg3) throws TException {
                        return null;
                    }

                    @Override
                    public String method_one(String arg1, DummyStruct arg2, boolean arg3)
                            throws TException {
                        return "dummy";
                    }
                };
                staticdummysvc = LitelinksService.createService(SettableThriftServiceImpl.class,
                        null, "name_isnt_used", publishPort, null, null);
                staticdummysvc.startAsync().awaitRunning();
                mappedsvc.awaitRunning();

                // make sure mapped service is listening on listeningPort
                DummyService.Iface client = ThriftClientBuilder.newBuilder(DummyService.Client.class)
                        .withStaticServer("localhost", listenPort, null, DefaultThriftServer.DEFAULT_FRAMED, false)
                        .buildOnceAvailable(3000);
                assertEquals("mapped", client.method_one("", null, false));

                // make sure mapped service registered on publishPort
                //    (this should 'discover' the mapped service but hit the static dummy service
                //     which is actually listening on that port)
                DummyService.Iface client2 = ThriftClientBuilder.newBuilder(DummyService.Client.class)
                        .withZookeeper(ZK).withServiceName("port_mapped_service").buildOnceAvailable(3000);
                assertEquals("dummy", client2.method_one("", null, false));

            } finally {
                stopAll(mappedsvc, staticdummysvc);
            }
        }
    }

    @Test
    public void static_registry_test() throws Exception {
        int port = 1082;

        assertNull("Don't run the unit tests with ZOOKEEPER env var set", System.getenv("ZOOKEEPER"));
        Service staticsvc = LitelinksService.createService(TestThriftServiceImpl.class,
                null, "name_isnt_used", port, null, null);
        try {
            DummyService.Iface client = ThriftClientBuilder.newBuilder(DummyService.Client.class)
                    .withStaticServer("localhost", port, null, DefaultThriftServer.DEFAULT_FRAMED, false).build();

            assertTrue(((LitelinksServiceClient) client).isAvailable()); // static always "available"

            staticsvc.startAsync().awaitRunning();

            basic_test(client, false);

        } finally {
            staticsvc.stopAsync().awaitTerminated();
        }
    }

    @Test
    public void service_class_mismatch_test() throws Exception {
        try {
            // try to create a client with wrong service class
            ThriftClientBuilder.<DummyService2.Iface>newBuilder(DummyService2.Client.class)
                    .withZookeeper(ZK).withServiceName(DummyService.class.getName())
                    .buildOnceAvailable(5000l).just_one_method("testdata");
            fail("client creation should have failed");
        } catch (RuntimeException e) {
            e.printStackTrace();
            assertTrue(e.getCause() != null && e.getCause().getMessage() != null
                    && e.getCause().getMessage().startsWith("service class mismatch"));
        }
        // try to start server instance with same svc name but different svc class
        Service svc = LitelinksService.createService(TestThriftService2Impl.class,
                ZK, DummyService.class.getName());
        svc.startAsync();
        try {
            svc.awaitRunning();
            svc.stopAsync();
            fail("service start should have failed");
        } catch (IllegalStateException ise) {
            assertTrue(ise.getCause() instanceof ConfiguredService.ConfigMismatchException);
        }
    }

    @Test
    public void service_subclass_test() throws Exception {
        String svcname = "subclass_svc";
        // start subclass
        Service svc = null, svc2 = null;
        try {
            svc = LitelinksService.createService(TestThriftService3Impl.class,
                    ZK, svcname).startAsync();
            // create and test a superclass client
            DummyService.Iface client = ThriftClientBuilder.<DummyService.Iface>newBuilder(DummyService.Client.class)
                    .withZookeeper(ZK).withServiceName(svcname)
                    .buildOnceAvailable(5000l);

            basic_test(client, false);

            svc2 = LitelinksService.createService(TestThriftServiceImpl.class,
                    ZK, svcname).startAsync();

            try {
                svc2.awaitRunning();
                fail("Adding superclass svc instnace should fail");
            } catch (IllegalStateException ise) {
                assertTrue(svc2.failureCause() instanceof ConfigMismatchException);
                svc2 = null; // else stop will fail
            }

        } finally {
            stopAll(svc, svc2);
        }
    }

    @Test
    public void availability_check_tests() throws Exception {
        String sname = "availtestsvc";
        final DummyService.Iface client = ThriftClientBuilder.newBuilder(DummyService.Client.class)
                .withZookeeper(ZK).withServiceName(sname).build();
        assertTrue(client instanceof LitelinksServiceClient);
        try (final LitelinksServiceClient wsc = (LitelinksServiceClient) client) {
            // availability checks prior to service existing
            assertFalse(wsc.isAvailable());
            assertFalse(wsc.awaitAvailable(500l)); // 500ms

            synchronized (SettableThriftServiceImpl.class) {
                // create service which has 5sec sleep in init method
                SettableThriftServiceImpl.initPause = 5000l;
                Service svc = LitelinksService.createService(SettableThriftServiceImpl.class, ZK, sname);
                assertFalse(wsc.isAvailable());
                long start = System.currentTimeMillis();
                svc.startAsync();
                Thread.sleep(1000l);
                // shouldn't be running after 1sec
                assertFalse(wsc.isAvailable());

                // test timeout case
                long before = System.currentTimeMillis();
                boolean avail = wsc.awaitAvailable(2500l);
                long took = System.currentTimeMillis() - before;
                assertFalse(avail);
                assertTrue(took > 2400 && took < 2600);

                // test successful wait case
                avail = wsc.awaitAvailable(10000l);
                took = System.currentTimeMillis() - start;
                assertTrue(avail);
                assertTrue("service became avail after " + took + "ms, expected ~5100", took > 5000 && took < 6000);
                assertTrue(wsc.isAvailable());

                // stop service and verify negative availability check
                svc.stopAsync().awaitTerminated();
                Thread.sleep(1000);
                assertFalse(wsc.isAvailable());

                // test buildOnceAvailable() timeout
                before = System.currentTimeMillis();
                try {
                    ThriftClientBuilder.newBuilder(DummyService.Client.class)
                            .withZookeeper(ZK).withServiceName("availtestsvc2").buildOnceAvailable(2000l);
                    fail("TimeoutException was not called");
                } catch (TimeoutException e) {
                    // should come here
                    took = System.currentTimeMillis() - before;
                    assertTrue(took > 2000l && took < 2300l);
                }
            }
        }
    }

    @Test
    public void anchor_file_tests() throws Exception {
        ExecutorService es = Executors.newSingleThreadExecutor();
        try {
            // test doesn't exist case
            Callable<Integer> c = new Callable<Integer>() {
                @Override
                public Integer call() throws Exception {
                    return LitelinksService.launch("-s", "com.ibm.watson.litelinks.test.TestThriftServiceImpl",
                            "-n", "dummyservice3", "-a", "noexist");
                }
            };
            Future<Integer> fut = es.submit(c);
            int rc = fut.get(1l, TimeUnit.SECONDS);
            assertEquals(rc, 2l); // should fail with exit code 2 (doesn't exist)

            final File anch = File.createTempFile("anchor-" + System.currentTimeMillis(), null);
            anch.createNewFile();
            anch.deleteOnExit();
            System.out.println("AF=" + anch);
            /*Callable<Integer>*/
            c = new Callable<Integer>() {
                @Override
                public Integer call() throws Exception {
                    return LitelinksService.launch("-s", "com.ibm.watson.litelinks.test.TestThriftServiceImpl",
                            "-n", "dummyservice3", "-a", anch.getAbsolutePath());
                }
            };

            /*Future<Integer>*/
            fut = es.submit(c);
            Thread.sleep(5000l); // wait 5sec
            assertFalse(fut.isDone()); // check still running
            anch.delete();
            System.out.println("AF deleted");

            rc = fut.get(8l, TimeUnit.SECONDS); // give 8sec to stop
            assertEquals(rc, 0);
        } finally {
            es.shutdownNow();
        }
    }

    private volatile long pause;

    @Test
    public void cluster_test() throws Exception {
        // make cluster mix of SSL/non-SSL and different protocols
        SSLMode[] sslMode = { SSLMode.NONE, SSLMode.ENABLED, SSLMode.NONE, SSLMode.ENABLED, SSLMode.CLIENT_AUTH };
        TProtocolFactory[] tpfacs = {
                null, new TJSONProtocol.Factory(), null,
                new TTupleProtocol.Factory(), new TBinaryProtocol.Factory()
        };
        final Service[] cluster = new Service[5];
        synchronized (SettableThriftServiceImpl.class) {
            SettableThriftServiceImpl.initPause = 0l;
            try {
                for (int i = 0; i < cluster.length; i++) {
                    final int fi = i;
                    SettableThriftServiceImpl.template = new DummyService.Iface() {
                        @Override
                        public DummyStruct method_two(int arg1, String arg2, ByteBuffer arg3) throws TException {
                            long start = System.nanoTime();
                            DummyStruct ds = new DummyStruct();
                            ds.setDoubleField(fi);
                            ds.setStringField(arg2);
                            long p = pause;
                            if (p > 0) {
                                try {
                                    Thread.sleep(ThreadLocalRandom.current()
                                            .nextLong(p)); //p/2 //java.util.concurrent.ThreadLocalRandom.
                                } catch (InterruptedException ie) {
                                    throw new TException(ie);
                                }
                            }
                            ds.setLongField(System.nanoTime() - start);
                            return ds;
                        }

                        @Override
                        public String method_one(String arg1, DummyStruct arg2, boolean arg3)
                                throws TException {
                            return null;
                        }
                    };
                    (cluster[i] =
                            LitelinksService.createService(new ServiceDeploymentConfig(SettableThriftServiceImpl.class)
                                    .setZkConnString(ZK).setServiceName("svccluster").setServiceVersion("version-" + i)
                                    .setProtoFactory(tpfacs[i]).setSslMode(sslMode[i])).startAsync()).awaitRunning();
                    System.out.println("started cluster member " + i);
                }

                Set<Integer> hit = new HashSet<>();
                final DummyService.Iface client = ThriftClientBuilder.newBuilder(DummyService.Client.class)
                        .withServiceName("svccluster").withZookeeper(ZK).withTimeout(3500).buildOnceAvailable(3000l);
                int max = 20 * cluster.length;
                for (int i = 1; i <= max; i++) {
                    hit.add((int) client.method_two(0, "", null).getDoubleField());
                    System.out.println("hitset = " + hit);
                    if (hit.size() == cluster.length) {
                        break;
                    }
                    assertNotEquals("Not all cluster members hit: " + hit, i, max);
                }

                // test custom loadbalancer function
                final DummyService.Iface lbClient = ThriftClientBuilder.newBuilder(DummyService.Client.class)
                        .withServiceName("svccluster").withZookeeper(ZK).withTimeout(3500)
                        .withLoadBalancer(new LoadBalancingPolicy.InclusiveLoadBalancingPolicy() {
                            @Override
                            public LoadBalancer getLoadBalancer() {
                                return new CustomLoadBalancer() {
                                    @Override
                                    public ServiceInstanceInfo getNext(List<ServiceInstanceInfo> list,
                                            String method, Object[] args) {
                                        assertEquals(cluster.length, list.size());
                                        assertEquals("method_two", method);
                                        assertEquals(33, args[0]);
                                        for (ServiceInstanceInfo sii : list) {
                                            if ("version-2".equals(sii.getVersion())) {
                                                return sii;
                                            }
                                        }
                                        return list.get(0); // should not reach here
                                    }
                                };
                            }
                        }).buildOnceAvailable(3000l);

                // all invocations should hit instance 2
                for (int i = 0; i < 8; i++) {
                    assertEquals(2, (int) lbClient.method_two(33, "", null).getDoubleField());
                }

                ServiceInstanceInfo[] returnFromLb = new ServiceInstanceInfo[] { null }; // reject all

                // test "non-inclusive" loadbalancer function
                final DummyService.Iface ilbClient = ThriftClientBuilder.newBuilder(DummyService.Client.class)
                        .withServiceName("svccluster").withZookeeper(ZK).withTimeout(3500)
                        .withLoadBalancer(() -> new CustomLoadBalancer() {
                            @Override
                            public ServiceInstanceInfo getNext(List<ServiceInstanceInfo> list,
                                    String method, Object[] args) {
                                return returnFromLb[0]; // reject all
                            }
                        }).buildOnceAvailable(3000l);

                // should throw SUE despite cluster containing instances
                try {
                    ilbClient.method_two(33, "", null);
                    fail("should throw SUE");
                } catch (TException te) {
                    assertTrue(te.getCause() instanceof ServiceUnavailableException);
                    assertFalse(te.getCause().getMessage().startsWith("request aborted by load balancer"));
                }

                returnFromLb[0] = LoadBalancer.ABORT_REQUEST; // reject all and don't fall back

                // test "non-inclusive" loadbalancer function using LoadBalancer.ABORT_REQUEST constant
                // should throw SUE despite cluster containing instances
                try {
                    ilbClient.method_two(33, "", null);
                    fail("should throw SUE");
                } catch (TException te) {
                    assertTrue(te.getCause() instanceof ServiceUnavailableException);
                    assertTrue(te.getCause().getMessage().startsWith("request aborted by load balancer"));
                }

                ((LitelinksServiceClient) lbClient).close();
                ((LitelinksServiceClient) ilbClient).close();

                // check unique version registration/discovery & registration times
                Set<String> versions = new HashSet<>();
                Set<Long> regTimes = new HashSet<>();
                for (ServiceInstanceInfo sii : ((LitelinksServiceClient) client).getServiceInstanceInfo()) {
                    String v = sii.getVersion();
                    assertNotNull(v);
                    versions.add(v);
                    long regTime = sii.getRegistrationTime(), now = System.currentTimeMillis();
                    assertTrue(regTime < now && regTime > now - 600000l);
                    regTimes.add(regTime);
                }
                assertEquals(cluster.length, versions.size());
                for (int i = 0; i < cluster.length; i++) {
                    assertTrue(versions.contains("version-" + i));
                }
                assertTrue(regTimes.size() > 1); // ensure the reg times aren't all the same

                pause = 1200;

                // Next test: multithread hammer with pause on server-side; add/remove cluster members
                Future<Integer>[] fs = new Future[11];
                ExecutorService es = Executors.newFixedThreadPool(fs.length);
                final AtomicLong totalTime = new AtomicLong();
                for (int i = 0; i < fs.length; i++) {
                    final int t = i;
                    fs[i] = es.submit(new Callable<Integer>() {
                        @Override
                        public Integer call() throws Exception {
                            int j = 0;
                            while (true) {
                                String msg = "thread " + t + " count " + j++;
                                long b4 = System.nanoTime();
                                DummyStruct ds = client.method_two(8, msg, null);
                                long took = System.nanoTime() - b4;
                                if (!msg.equals(ds.getStringField())) {
                                    throw new Exception(
                                            "Data mismatch: '" + msg + "' != '" + ds.getStringField() + "'");
                                }
                                totalTime.addAndGet(took - ds.getLongField());
                                if (pause < 0) {
                                    return j;
                                }
                            }
                        }
                    });
                }

                Thread.sleep(8000);

                for (int i = 0; i < 3; i++) {
                    long before = System.currentTimeMillis();
                    cluster[i].stopAsync().awaitTerminated();
                    long took = System.currentTimeMillis() - before;
                    System.out.println("cluster member " + i + " shutdown took " + took + "ms");
                    cluster[i] = null;
                    Thread.sleep(3500);
                }

                Thread.sleep(4500);

                pause = -1;
                int totCount = 0;
                for (int i = 0; i < fs.length; i++) {
                    int count = fs[i].get();
                    totCount += count;
                    assertTrue("thread completed fewer invocations than expected (" + count + " <= 20)",
                            count > 20);
                }
                long avgOverhead = totalTime.get() / (totCount * 1000l);
                System.out.println("Average roundtrip overhead = " + avgOverhead + "µs (count=" + totCount + ")");
                assertTrue("Average roundtrip overhead > 10ms (" + avgOverhead + "µs)",
                        avgOverhead < 10000); // was 8000
            } finally {
                stopAll(cluster);
            }
        }
    }


    @Test
    public void large_payload_test() throws Exception {
        final DummyService.Iface client = ThriftClientBuilder.newBuilder(DummyService.Client.class)
                .withZookeeper(ZK).withServiceName("dummyservice2").withTimeout(3000).buildOnceAvailable(5000l);
        int size = 10000000; // ten meeleon chars .. about 20MB string
        char[] chars = new char[size];
        Random r = new Random(System.currentTimeMillis());
        for (int i = 0; i < size; i++) {
            chars[i] = (char) (' ' + r.nextInt(95));
        }
        for (int i = 0; i < 8; i++) {
            basic_test2(client, new String(chars), false, false);
        }
    }

    @Test
    public void random_strings_test() throws Exception {
        final DummyService.Iface client = ThriftClientBuilder.newBuilder(DummyService.Client.class)
                .withZookeeper(ZK).withServiceName("dummyservice2").withTimeout(30000).buildOnceAvailable(5000l);
        Random r = new Random(33L);
        byte[] bytes = new byte[51200];
        r.nextBytes(bytes);
        System.out.println("generated randoms");
        for (int t = 0; t < 2000; t++) {
            basic_test2(client,
                    new String(bytes, 0, r.nextInt(bytes.length), StandardCharsets.UTF_8),
                    false, false);
            ReleaseAfterResponse.releaseAll();
        }
    }

    @Test
    public void large_payload_ssl_test() throws Exception {
        final DummyService.Iface client = ThriftClientBuilder.newBuilder(DummyService.Client.class)
                .withZookeeper(ZK).withServiceName("dummysslservice").withTimeout(3000).buildOnceAvailable(5000l);
        int size = 10000000; // ten meeleon chars .. about 20MB string
        char[] chars = new char[size];
        Random r = new Random(33L);
        for (int i = 0; i < size; i++) {
            chars[i] = (char) (' ' + r.nextInt(95));
        }
        for (int i = 0; i < 8; i++) {
            basic_test2(client, new String(chars), false, false);
        }
    }

    @Test
    public void new_service_test() throws Exception {
        /*
         * Scenario in this test:
         * 1- client created for service which doesn't exist
         * 2- client service invocation should fail with unavailable
         * 3- service instance started, client should detect
         * 4- client service invocation should succeed
         * 5- service instance stopped; service znode deleted
         *     (while client still running)
         * 6- service instance started again (with diff config),
         *    client should detect that service node has returned
         *    with different config and successfully invoke the service
         * 7- verify service node has been deleted after last
         *    instance has stopped
         *
         */
        final String servicename = "name_which_doesnt_already_exist";
        CuratorFramework curator = ZookeeperClient.getCurator(ZK, true);
        try {
            curator.delete().forPath("/services/" + servicename);
        } catch (KeeperException.NoNodeException e) {
        }
        ExecutorService executor = Executors.newCachedThreadPool();
        try {
            DummyService.Iface client = ThriftClientBuilder.newBuilder(DummyService.Client.class)
                    .withZookeeper(ZK).withServiceName(servicename).withTimeout(6000)
                    .withListener(new AvailabilityListener() {
                        @Override
                        public void instancesChanged(String sn, List<ServiceInstanceInfo> instanceInfos) {
                            System.out.println("listener: instances changed sn=" + sn + ", list=" + instanceInfos);
                        }

                        @Override
                        public void availabilityChanged(String sn, boolean available) {
                            System.out.println("listener: availability changed sn=" + sn + " newval=" + available);
                        }
                    }, executor).build();
            try {
                basic_test(client);
                fail("ServiceUnavailableException should have been thrown");
            } catch (TException e) {
                assertTrue(e.getCause() instanceof ServiceUnavailableException);
            }
            Thread.sleep(2000);
            Service newSvc = LitelinksService.createService(TestThriftServiceImpl.class, ZK, servicename);
            newSvc.startAsync(); //.awaitRunning();
//		Thread.sleep(1000);
            assertTrue("availability timeout", ((LitelinksServiceClient) client).awaitAvailable(4000l));
            basic_test(client, false);
            newSvc.stopAsync().awaitTerminated();
            try {
                curator.delete().forPath("/services/" + servicename);
            } catch (KeeperException.NoNodeException e) {
            }
            try {
                basic_test(client);
                fail("ServiceUnavailableException should have been thrown");
            } catch (TException e) {
                assertTrue("Unexpected Exception: " + e, e.getCause() instanceof ServiceUnavailableException);
            }
            Thread.sleep(2000);
            newSvc = LitelinksService.createService(TestThriftServiceImpl.class,
                    ZK, servicename, -1, new TJSONProtocol.Factory(), SSLMode.ENABLED);
            newSvc.startAsync().awaitRunning();
//		Thread.sleep(1000);
            assertTrue("availability timeout", ((LitelinksServiceClient) client).awaitAvailable(4000L));
            assertTrue(((LitelinksServiceClient) client).isAvailable());
            basic_test(client, false);

            // must close client prior to stopping the service for the final test to work
            // (deletion of service znode) if curator 4.0.1+ is used with zookeeper 3.4.x;
            // otherwise the service znode will get recreated by the PathChildrenCache
            // due to https://github.com/apache/curator/pull/249
            ((Closeable) client).close();

            newSvc.stopAsync().awaitTerminated();

            // service parent znode should get cleaned up when last child (instance)
            // deregisters - make sure its gone
            assertNull(curator.checkExists().forPath("/services/" + servicename));
        } finally {
            executor.shutdown();
        }
    }

    @Test
    public void timeout_test() throws Exception {
        synchronized (ChangeableThriftServiceImpl.class) {
            ChangeableThriftServiceImpl.delegate = new DummyService.Iface() {
                @Override
                public DummyStruct method_two(int arg1, String arg2, ByteBuffer arg3) throws TException {
                    long dline = ThreadContext.nanosUntilDeadline();
                    if (-1L != dline) {
                        throw new TException("unexpected deadline encountered: " + dline);
                    }
                    try {
                        Thread.sleep(3000); // 3sec
                    } catch (InterruptedException e) {
                        throw new TException(e);
                    }
                    return new DummyStruct();
                }

                @Override
                public String method_one(String arg1, DummyStruct arg2, boolean arg3) throws TException {
                    if (ThreadContext.isDeadlineExpired()) {
                        throw new TException("unexpected deadline expired");
                    }
                    try {
                        Thread.sleep(1000); // 1sec
                    } catch (InterruptedException e) {
                        throw new TException(e);
                    }
                    return "result";
                }
            };

            DummyService.Iface client1 = ThriftClientBuilder.newBuilder(DummyService.Client.class)
                    .withZookeeper(ZK).withTimeout(1200).buildOnceAvailable(5000l);

            long b4 = System.nanoTime();
            client1.method_one("", null, false); // should succeed
            long took = (System.nanoTime() - b4) / 1000;
            try {
                b4 = System.nanoTime();
                client1.method_two(4, "", null);
                took = (System.nanoTime() - b4) / 1000_000L;
                fail("Timeout exception should have been thrown (took " + took + "ms)"); // shouldn't reach here
            } catch (TTransportException e) {
                took = (System.nanoTime() - b4) / 1000;
//				if(TTransportException.TIMED_OUT != e.getType()) e.printStackTrace();
                assertTrue(TTransportException.TIMED_OUT == e.getType()
                        || TTransportException.UNKNOWN == e.getType()
                        && e.getCause() instanceof SocketTimeoutException);
                assertTrue("took " + took / 1000 + "ms, expected ~1200", took > 1198000 && took < 1280000);
            }
        }
    }

    @Test
    public void deadline_test() throws Exception {
        synchronized (ChangeableThriftServiceImpl.class) {
            ChangeableThriftServiceImpl.delegate = new DummyService.Iface() {
                @Override
                public DummyStruct method_two(int arg1, String arg2, ByteBuffer arg3) throws TException {
                    long ms = ThreadContext.nanosUntilDeadline() / 1000_000L;
                    if (ms < 1150L || ms > 1250L) {
                        throw new TException("unexpected deadline C of " + ms + "ms");
                    }
                    try {
                        Thread.sleep(3000); // 3sec
                    } catch (InterruptedException e) {
                        throw new TException(e);
                    } finally {
                        if (!ThreadContext.isDeadlineExpired()) {
                            System.err.println(
                                    "should be expired here, remaining is " + ThreadContext.nanosUntilDeadline());
                        }
                    }
                    return new DummyStruct();
                }

                @Override
                public String method_one(String arg1, DummyStruct arg2, boolean arg3) throws TException {
                    long ms = ThreadContext.nanosUntilDeadline() / 1000_000L;
                    if (ms < 1100L || ms > 1250L) {
                        throw new TException("unexpected deadline A of " + ms + "ms");
                    }
                    try {
                        Thread.sleep(1000); // 1sec
                    } catch (InterruptedException e) {
                        throw new TException(e);
                    }
                    ms = ThreadContext.nanosUntilDeadline() / 1000_000L;
                    if (ms < 100L || ms > 250L) {
                        throw new TException("unexpected deadline B of " + ms + "ms");
                    }
                    return "result";
                }
            };

            // disable client-level timeout
            DummyService.Iface client1 = ThriftClientBuilder.newBuilder(DummyService.Client.class)
                    .withZookeeper(ZK).withTimeout(0 /*disable*/).buildOnceAvailable(5000l);
            try {
                ThreadContext.setDeadlineAfter(1200, TimeUnit.MILLISECONDS);
                long b4 = System.nanoTime();
                client1.method_one("", null, false); // should succeed
                long took = (System.nanoTime() - b4) / 1000;
                ThreadContext.setDeadlineAfter(1200, TimeUnit.MILLISECONDS);
                try {
                    b4 = System.nanoTime();
                    client1.method_two(4, "", null);
                    fail("Timeout exception should have been thrown"); // shouldn't reach here
                } catch (TTransportException e) {
                    took = (System.nanoTime() - b4) / 1000;
                    //	              if(TTransportException.TIMED_OUT != e.getType()) e.printStackTrace();
                    assertTrue(TTransportException.TIMED_OUT == e.getType()
                            || TTransportException.UNKNOWN == e.getType()
                            && e.getCause() instanceof SocketTimeoutException);
                    assertTrue("took " + took / 1000 + "ms, expected ~1200", took > 1198000 && took < 1280000);
                }
            } finally {
                ThreadContext.removeDeadline();
            }
        }
    }

    //TODO we could still include a test for this - it now applies only if the
    // -Dlitelinks.force_homog_conn_props=true jvm arg is set
//	@Test
//	public void config_clash_test() throws Exception {
//		try {
//			// different protocol factory
//			Service svc = LitelinksService.createService(TestThriftServiceImpl.class,
//					ZK, "dummysslservice", -1, new TJSONProtocol.Factory(), SSLMode.ENABLED).startAsync();
//			svc.awaitRunning();
//			svc.stopAsync();
//			assertTrue("Config mismatch exception should have been thrown", false); // shouldn't reach here
//		} catch(Exception e) {
//			if(!(e.getCause() instanceof ConfigMismatchException)) throw e;
//		}
//	}

    @Test
    public void basic_test() throws Exception {
        DummyService.Iface client = ThriftClientBuilder.newBuilder(DummyService.Client.class)
                .withZookeeper(ZK).withTimeout(5000).buildOnceAvailable(5000l);
        try (LitelinksServiceClient lsc = (LitelinksServiceClient) client) {
            basic_test(client);
        }
    }

    static class ThriftCallbackFuture<V> extends AbstractFuture<V> implements AsyncMethodCallback<V> {
        @Override
        public void onComplete(V response) {
            logCallback();
            set(response);
        }

        @Override
        public void onError(Exception exception) {
            logCallback();
            setException(exception);
        }

        private ThriftCallbackFuture() {}

        public static <V> ThriftCallbackFuture<V> create() {
            return new ThriftCallbackFuture<>();
        }

        private static void logCallback() {
            System.out.println("async callback from thread " + Thread.currentThread().getName()
                    + " (id=" + Thread.currentThread().getId() + ")");
        }
    }

    @Test
    public void async_test() throws Exception {
        DummyService.Iface client = ThriftClientBuilder.newBuilder(DummyService.Client.class)
                .withZookeeper(ZK).withTimeout(5000).buildOnceAvailable(5000l);
        try (LitelinksServiceClient lsc = (LitelinksServiceClient) client) {
            basic_test_async(client);
        }
    }

    @Test
    public void async_err_test() throws Exception {
        DummyService.Iface client = ThriftClientBuilder.newBuilder(DummyService.Client.class)
                .withZookeeper(ZK).withServiceName("noexist").withTimeout(5000).build();
        try (LitelinksServiceClient lsc = (LitelinksServiceClient) client) {
            ThriftCallbackFuture<String> callback = ThriftCallbackFuture.create();
            ((DummyService.AsyncIface) client).method_one("hi", null, true, callback);
            try {
                callback.get();
                fail("error not thrown");
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof TException);
                assertTrue(e.getCause().getCause() instanceof ServiceUnavailableException);
            }
        }
    }

    @Test
    public void bbuffer_tests() throws Exception {
        DummyService.Iface client = ThriftClientBuilder.newBuilder(DummyService.Client.class)
                .withZookeeper(ZK).withTimeout(5000).buildOnceAvailable(5000l);
        try (LitelinksServiceClient lsc = (LitelinksServiceClient) client) {

            // ensure ByteBuffer positions don't change when sent by client

            ByteBuffer bbSmall = ByteBuffer.wrap("small"
                    .getBytes(StandardCharsets.UTF_8));
            int pos = bbSmall.position(), rem = bbSmall.remaining();
            client.method_two(5, "hi", bbSmall);
            assertEquals(pos, bbSmall.position());
            assertEquals(rem, bbSmall.remaining());

            // make buffer longer than NettyTTransport.WRITE_DIRECT_LEN bytes
            ByteBuffer bbLarge = ByteBuffer.wrap(Strings.repeat("long", 64)
                    .getBytes(StandardCharsets.UTF_8));
            pos = bbLarge.position();
            rem = bbLarge.remaining();
            client.method_two(5, "hi", bbLarge);
            assertEquals(pos, bbLarge.position());
            assertEquals(rem, bbLarge.remaining());
        }
    }

    @Test
    public void client_close_test() throws Exception {
        String svcname = "closesvc";
        Service closesvc = LitelinksService.createService(SimpleThriftServiceImpl.class,
                ZK, svcname).startAsync();
        try {
            DummyService.Iface client = ThriftClientBuilder.newBuilder(DummyService.Iface.class)
                    .withZookeeper(ZK).withServiceName(svcname).withTimeout(5000).buildOnceAvailable(5000l);
            DummyService.Iface client2 = ThriftClientBuilder.newBuilder(DummyService.Iface.class)
                    .withZookeeper(ZK).withServiceName(svcname).withTimeout(1000).build();
            basic_test(client, false);
            basic_test(client2, false);
            ((LitelinksServiceClient) client).close();

            for (int i = 0; i < 10; i++) {
                ((LitelinksServiceClient) client).close();
            }
            basic_test(client2, false);

            // close underlying client manager
            closeTSCMsForService(svcname);

            // this should succeed (auto-refresh client manager)
            basic_test(client2, false);

            try {
                ((LitelinksServiceClient) client).isAvailable(); // should throw
                fail("not thrown");
            } catch (ClientClosedException cce) {
            }
            try {
                basic_test(client, false); // should throw
                fail("not thrown");
            } catch (ClientClosedException cce) {
            }

            basic_test(client2, false);
            ((LitelinksServiceClient) client2).close();

            if (!TServiceClientManager.DELAY_CLOSING) {
                try {
                    ((LitelinksServiceClient) client2).getServiceInstanceInfo(); // should throw
                    fail("not thrown");
                } catch (ClientClosedException cce) {
                }
                try {
                    basic_test(client2, false); // should throw
                    fail("not thrown");
                } catch (ClientClosedException cce) {
                }
            }
        } finally {
            stopAll(closesvc);
        }
    }

    static {
        LoadingCache<?, TServiceClientManager<?>> cmc = null;
        Method m = null;
        try {
            Field f = TServiceClientManager.class.getDeclaredField("clientMgrCache");
            f.setAccessible(true);
            cmc = (LoadingCache<?, TServiceClientManager<?>>) f.get(null);
            m = TServiceClientManager.class.getDeclaredMethod("doClose", (Class<?>[]) null);
            m.setAccessible(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
        TSCMclientMgrCache = cmc;
        TSCMdoClose = m;
    }

    private static final LoadingCache<?, TServiceClientManager<?>> TSCMclientMgrCache;
    private static final Method TSCMdoClose;

    private static void closeTSCMsForService(String serviceName)
            throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        for (TServiceClientManager<?> tscm : TSCMclientMgrCache.asMap().values()) {
            if (serviceName.equals(tscm.getServiceName())) {
                System.out.println("Closing TSCM: " + tscm);
                TSCMdoClose.invoke(tscm, (Object[]) null);
            }
        }
    }


    @Test
    public void ssl_test() throws Exception {
        DummyService.Iface client = ThriftClientBuilder.newBuilder(DummyService.Client.class)
                .withZookeeper(ZK).withTimeout(5000).withServiceName("dummysslservice").buildOnceAvailable(5000l);
        basic_test(client, false);
    }

    @Test
    public void ssl_clientauth_test() throws Exception {
        DummyService.Iface client = ThriftClientBuilder.newBuilder(DummyService.Client.class)
                .withZookeeper(ZK).withTimeout(5000).withServiceName("dummysslcaservice").buildOnceAvailable(5000l);
        basic_test(client, false);
    }

    @Test
    public void server_exception_tests() throws Exception {
        DummyService.Iface client = ThriftClientBuilder.newBuilder(DummyService.Client.class)
                .withZookeeper(ZK).withTimeout(5000).buildOnceAvailable(5000l);
        exception_tests(client);
    }

    @Test
    public void multithread_test() throws Exception {
        final DummyService.Iface client = ThriftClientBuilder.newBuilder(DummyService.Client.class)
                .withZookeeper(ZK).withServiceName("dummyservice2").withTimeout(5000).buildOnceAvailable(5000l);
        Future<Void>[] fs = new Future[12];
        ExecutorService es = Executors.newFixedThreadPool(fs.length);
        for (int i = 0; i < fs.length; i++) {
            final int ii = i;
            fs[i] = es.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    for (int j = 0; j < 32; j++) {
                        basic_test2(client, "arg-" + ii + "-" + j, false, true);
                    }
                    return null;
                }
            });
        }
        for (Future<Void> f : fs) {
            f.get();
        }
    }


    public static void exception_tests(DummyService.Iface client) throws Exception {

        final TException[] throwme = new TException[1];
        final RuntimeException[] throwme2 = new RuntimeException[1];

        synchronized (ChangeableThriftServiceImpl.class) {
            ChangeableThriftServiceImpl.delegate = new DummyService.Iface() {
                @Override
                public String method_one(String arg1, DummyStruct arg2, boolean arg3) throws TException {
                    if (throwme[0] != null) {
                        throw throwme[0];
                    }
                    throw throwme2[0];
                }

                @Override
                public DummyStruct method_two(int arg1, String arg2, ByteBuffer arg3) {return null;}
            };


            throwme[0] = new TException("message1");
            Throwable out = callAndCatch(client);
            assertTrue(out instanceof TApplicationException);
            assertNotNull(out.getMessage());
            assertTrue(out.getMessage().contains("message1"));

            throwme[0] = new TestException("testexceptionmessage");
            out = callAndCatch(client);
            assertTrue(out instanceof TestException);
            assertEquals(out.getMessage(), "testexceptionmessage");

            throwme[0] = null;
            throwme2[0] = new IllegalArgumentException("iae");
            out = callAndCatch(client);
            assertTrue(out instanceof TApplicationException);
            assertNotNull(out.getMessage());
            assertTrue(out.getMessage().contains("iae"));
        }
    }

    private static Throwable callAndCatch(DummyService.Iface client) {
        try {
            client.method_one("test", null, false);
        } catch (Throwable t) {
            return t;
        }
        return null;
    }

    public static void basic_test(DummyService.Iface client) throws TException {
        basic_test(client, true);
    }

    public static void basic_test(DummyService.Iface client, boolean verifyCount) throws TException {
        synchronized (ChangeableThriftServiceImpl.class) {
            ChangeableThriftServiceImpl.delegate = defaultImpl;
            long b4 = System.nanoTime();
            DummyStruct ds;
            ByteBuffer bb = ByteBuffer.wrap("fourth".getBytes());
            if (verifyCount) {
                ((TestThriftServiceImpl) defaultImpl).hitCount.set(0);
            }
            try {
                ds = client.method_two(5, "third", bb.duplicate());
            } finally {
                System.out.println("took " + (System.nanoTime() - b4) / 1000 + "µs");
            }
            if (verifyCount) {
                assertEquals(1, ((TestThriftServiceImpl) defaultImpl).hitCount.get());
            }
            assertNotNull(ds);
//			assertNull(ds.getStringField());
            assertEquals("defaultString", ds.getStringField());
            assertEquals(DummyEnum.OPTION2, ds.getEnumField());
            assertNotNull(ds.getListField());
            assertEquals(4, ds.getListField().size());
            assertEquals("fourth", ds.getListField().get(3));
            assertEquals(bb, ds.bufferForBinaryField());
        }
    }

    public static void basic_test_async(DummyService.Iface client) throws Exception {
        DummyService.AsyncIface async = (DummyService.AsyncIface) client;
        ThriftCallbackFuture<DummyStruct> callback = ThriftCallbackFuture.create();
        synchronized (ChangeableThriftServiceImpl.class) {
            ChangeableThriftServiceImpl.delegate = defaultImpl;
            long b4 = System.nanoTime();
            DummyStruct ds;
            ByteBuffer bb = ByteBuffer.wrap("fourth".getBytes());
            ((TestThriftServiceImpl) defaultImpl).hitCount.set(0);
            async.method_two(5, "third", bb.duplicate(), callback);
            try {
                ds = callback.get();
            } finally {
                System.out.println("took " + (System.nanoTime() - b4) / 1000 + "µs");
            }
            assertEquals(1, ((TestThriftServiceImpl) defaultImpl).hitCount.get());
            assertNotNull(ds);
            assertEquals("defaultString", ds.getStringField());
            assertEquals(DummyEnum.OPTION2, ds.getEnumField());
            assertNotNull(ds.getListField());
            assertEquals(4, ds.getListField().size());
            assertEquals("fourth", ds.getListField().get(3));
            assertEquals(bb, ds.bufferForBinaryField());
        }
    }

    public static void basic_test2(DummyService.Iface client, final String input,
            boolean printtime, boolean sleep) throws Exception {
        long b4 = printtime ? System.nanoTime() : 0l;
        ByteBuffer bb = ByteBuffer.wrap(input.getBytes());
        DummyStruct ds = client.method_two(sleep ? 1 : 0, input, bb.duplicate());
        if (printtime) {
            long took = System.nanoTime() - b4, inside = ds.getLongField();
            System.out.println("took " + took / 1000 + "µs with overhead=" + (took - inside) / 1000 + "µs");
        }
        assertNotNull(ds);
//		assertNull(ds.getStringField());
        assertEquals("defaultString", ds.getStringField());
        assertEquals(DummyEnum.OPTION2, ds.getEnumField());
        assertNotNull(ds.getListField());
        assertEquals(4, ds.getListField().size());
        assertEquals(input, ds.getListField().get(3));
        assertEquals(input, ds.getListField().get(2));
        assertEquals(bb, ds.bufferForBinaryField());
    }

    public static void stopAll(Service... services) {
        for(Service s : services) if(s!=null) s.stopAsync();
        RuntimeException failure = null;
        for(Service s : services) if(s!=null) try {
            s.awaitTerminated();
        } catch(RuntimeException e) { failure = e; }
        if (failure != null) throw failure;
    }

}
