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

import com.google.common.collect.ImmutableMap;
import com.ibm.watson.litelinks.LitelinksEnvVariableNames;
import com.ibm.watson.litelinks.LitelinksSystemPropNames;
import com.ibm.watson.litelinks.SSLHelper.SSLParams;
import com.ibm.watson.litelinks.TConnectionClosedException;
import com.ibm.watson.litelinks.client.ClientTTransportFactory;
import com.ibm.watson.litelinks.client.LitelinksServiceClient;
import com.ibm.watson.litelinks.client.LitelinksServiceClient.ServiceInstanceInfo;
import com.ibm.watson.litelinks.client.ThriftClientBuilder;
import com.ibm.watson.litelinks.server.DefaultThriftServer;
import com.ibm.watson.litelinks.server.LitelinksService;
import com.ibm.watson.litelinks.test.thrift.DummyService;
import com.ibm.watson.litelinks.test.thrift.DummyService.Iface;
import com.ibm.watson.zk.ZookeeperClient;
import org.apache.curator.test.TestingServer;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.PrintWriter;
import java.lang.ProcessBuilder.Redirect;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.*;

public class LitelinksLauncherTests {

    public static String ZK; // = System.getenv("ZOOKEEPER");
    private static TestingServer localZk;

    public static Class<?> watsonSvcClass = LitelinksService.class;

    @BeforeClass
    public static synchronized void beforeClass() throws Exception {
        if (ZK == null || "".equals(ZK.trim())) {
            localZk = new TestingServer();
            ZK = localZk.getConnectString();
        }
        System.out.println("Using ZK connstring: " + ZK);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        // disconnect ZK clients before stopping ZK
        ZookeeperClient.shutdown(false, false);
        Thread.sleep(3000);
        // stop ZK
        if (localZk != null) {
            localZk.stop();
        }
    }

    /*
     * TODO:
     *   - Test other cmd line args (public port, anchor file)
     *   - Test other env vars (hostname, public port)
     *   - Test killing proc (e.g. dereg shutdown hook, etc)
     *   - Other (startup) failure cases
     */

    /**
     * http health probe test
     */
    @Test(timeout = 10000)
    public void probe_test() throws Exception {
        String sname = "probe_test";
        String healthcheckPort = "1100";
        DummyService.Iface client = ThriftClientBuilder.newBuilder(DummyService.Client.class)
                .withZookeeper(ZK).withServiceName(sname).withTimeout(5000).build();
        runTest(TestThriftServiceImpl.class, client, new Tester<DummyService.Iface>() {
            @Override
            public void test(Iface client) throws Exception {
                HttpClient hclient = HttpClient.newHttpClient();
                HttpRequest readyRequest = HttpRequest.newBuilder()
                        .uri(URI.create("http://localhost:" + healthcheckPort + "/ready")).build();
                HttpRequest liveRequest = HttpRequest.newBuilder()
                        .uri(URI.create("http://localhost:" + healthcheckPort + "/live")).build();
                assertEquals(200, hclient.send(readyRequest, BodyHandlers.discarding()).statusCode());
                assertEquals(200, hclient.send(liveRequest, BodyHandlers.discarding()).statusCode());
                ((LitelinksServiceClient) client).testConnection();
                // This will set the service to not-live
                assertEquals("OK", client.method_one("setlive", null, false));
                // Liveness probe should now fail with internal server error
                assertEquals(500, hclient.send(liveRequest, BodyHandlers.discarding()).statusCode());
                assertEquals(200, hclient.send(readyRequest, BodyHandlers.discarding()).statusCode());
                // This will set the service to not-ready
                assertEquals("OK", client.method_one("setready", null, false));
                // Liveness probe should now fail with service unavailable
                assertEquals(503, hclient.send(readyRequest, BodyHandlers.discarding()).statusCode());
            }
        }, true, null, null, "-z", ZK, "-n", sname, "-h", healthcheckPort);
    }

    /**
     * basic start / invoke / stop
     */
    @Test
    public void external_launch_test() throws Exception {
        String sname = "launched_test";
        DummyService.Iface client = ThriftClientBuilder.newBuilder(DummyService.Client.class)
                .withZookeeper(ZK).withServiceName(sname).withTimeout(5000).build();
        runTest(client, true, null, "-z", ZK, "-n", sname);
    }

    private static final String[] NONE = new String[0];

    @Test
    public void external_ssl_test() throws Exception {
        final Class<?> C = LitelinksLauncherTests.class;
        final String keystorePath = C.getResource("/keystore.jks").getFile();
        final String keyPath = C.getResource("/key.pem").getFile();
        final String key2Path = C.getResource("/key2.pem").getFile();
        final String key3EncPath = C.getResource("/key3-enc.pem").getFile();
        final String certDir = C.getResource("/certs").getFile();
        final String certPath = C.getResource("/certs/cert.pem").getFile();
        final String cert2Path = C.getResource("/certs/cert2.pem").getFile();
        final String cert3Path = C.getResource("/certs/cert3.pem").getFile();
        // truststore + keystore
        do_ssl_test(arr("truststore.path", keystorePath, "truststore.password", "password"),
                arr("keystore.path", keystorePath, "keystore.password", "password"));
        // with pems
        do_ssl_test(arr("trustcerts.path", certPath), arr("key.path", keyPath, "key.certpath", certPath));
        // with pems and cert dir
        do_ssl_test(arr("trustcerts.path", certDir), arr("key.path", keyPath, "key.certpath", certPath));
        // with pems and cert dir, different key, using env vars
        do_ssl_test(arr("trustcerts.path", certDir), NONE,
                arr("KEY_PATH", key2Path, "KEY_CERTPATH", cert2Path));
        // with encrypted key
        do_ssl_test(arr("trustcerts.path", certDir),
                arr("key.path", key3EncPath, "key.certpath", cert3Path, "key.password", "testpass"));
        // with truststore + cert file, pem key
        do_ssl_test(arr("trustcerts.path", certPath, "truststore.path",
                keystorePath, "truststore.password", "password"),
                arr("key.path", keyPath, "key.certpath", certPath));
        // with truststore + certs dir, pem key
        do_ssl_test(arr("trustcerts.path", certDir, "truststore.path",
                keystorePath, "truststore.password", "password"),
                arr("key.path", keyPath, "key.certpath", certPath));
        // with truststore + certs dir, keystore, using env vars
        do_ssl_test(arr("trustcerts.path", certDir, "truststore.path",
                keystorePath, "truststore.password", "password"), NONE,
                arr("KEYSTORE_PATH", keystorePath, "KEYSTORE_PASSWORD", "password"));
    }

    @Test
    public void external_ssl_ca_test() throws Exception {
        final Class<?> C = LitelinksLauncherTests.class;
        final String keyPath = C.getResource("/key.pem").getFile();
        final String key2Path = C.getResource("/key2.pem").getFile();
        final String certPath = C.getResource("/certs/cert.pem").getFile();
        final String cert2Path = C.getResource("/certs/cert2.pem").getFile();

        // with pems - mutual auth
        do_ssl_ca_test(arr("trustcerts.path", cert2Path, "key.path", keyPath, "key.certpath", certPath),
                arr("trustcerts.path", certPath, "key.path", key2Path, "key.certpath", cert2Path));
    }

    private static void do_ssl_test(String[] clientargs, String[] serverargs,
            String... serverEvs) throws Exception {
        do_ssl_test0("ssl", clientargs, serverargs, serverEvs);
    }

    private static void do_ssl_ca_test(String[] clientargs, String[] serverargs,
            String... serverEvs) throws Exception {
        do_ssl_test0("ssl-ca", clientargs, serverargs, serverEvs);
    }

    private static void do_ssl_test0(String mode, String[] clientargs, String[] serverargs,
            String... serverEvs) throws Exception {
        SSLParams.resetDefaultParameters();
        try {
            for (int i = 0; i < clientargs.length; i += 2) {
                System.setProperty("litelinks.ssl." + clientargs[i], clientargs[i + 1]);
            }
            List<String> argList = new ArrayList<>(serverargs.length / 2);
            for (int i = 0; i < serverargs.length; i += 2) {
                argList.add("-Dlitelinks.ssl." + serverargs[i] + "=" + serverargs[i + 1]);
            }
            Map<String, String> envVarMap = new HashMap<>(serverEvs.length + 1);
            envVarMap.put("WATSON_SERVICE_ADDRESS", "localhost");
            for (int i = 0; i < serverEvs.length; i += 2) {
                envVarMap.put("LITELINKS_SSL_" + serverEvs[i], serverEvs[i + 1]);
            }
            String sname = "launched_test";
            DummyService.Iface client = ThriftClientBuilder.newBuilder(DummyService.Client.class)
                    .withZookeeper(ZK).withServiceName(sname).withTimeout(5000).build();
            try (LitelinksServiceClient lsClient = (LitelinksServiceClient) client) {
                runTest(TestThriftServiceImpl.class, client, true, envVarMap,
                        argList.toArray(new String[0]), "-z", ZK, "-n", sname, "-e", mode);
            }
        } finally {
            for (int i = 0; i < clientargs.length; i += 2) {
                System.clearProperty("litelinks.ssl." + clientargs[i]);
            }
        }
    }

    public static String[] arr(String... arr) { return arr; }

    /**
     * shutdown timeout test
     */
    @Test
    public void preshutdown_and_shutdown_timeout_test() throws Exception {
        String sname = "shutdown_timeout_test";
        DummyService.Iface client = ThriftClientBuilder.newBuilder(DummyService.Client.class)
                .withZookeeper(ZK).withServiceName(sname).withTimeout(5000).build();
        File tmpFile = File.createTempFile("shutdowntmp_" + System.currentTimeMillis(), ".tmp");
        tmpFile.deleteOnExit();
        ProcessBuilder builder = buildSvcProcess(ShutdownTestThriftServiceImpl.class,
                null, new String[] {
                        "-Dlitelinks.shutdown_timeout_ms=3200",
                        "-Dtmpfile=" + tmpFile.getAbsolutePath()
                },
                "-z", ZK, "-n", sname);
        // start the service
        final Process svcProc = builder.start();
        try {
            PrintWriter pw = new PrintWriter(svcProc.getOutputStream());
            // wait to become available
            assertTrue("Timeout waiting for service to become available",
                    ((LitelinksServiceClient) client).awaitAvailable(8000l));
            // run basic test with client
            LitelinksTests.basic_test(client, false);
            assertTrue(tmpFile.exists());
            System.out.println("sending stop");
//			// send SIGTERM
//			svcProc.destroy();
            pw.println("stop");
            pw.flush();
            long b4 = System.currentTimeMillis();
            Thread.sleep(1000L);
            // ensure that preShutdown was run by checking the file
            assertFalse(tmpFile.exists());
            // ensure that the service is still active/working in the preShutdown phase
            LitelinksTests.basic_test(client, false);
            // ensure the overall shutdown times out at about 3200ms
            Thread.sleep(3000L + b4 - System.currentTimeMillis());
            assertTrue(isRunning(svcProc));
            Thread.sleep(800L);
            assertFalse(isRunning(svcProc));
            assertFalse(((LitelinksServiceClient) client).isAvailable());
        } finally {
            ((LitelinksServiceClient) client).close();
            svcProc.destroy(); // should already be gone in success cases
        }
    }

    /**
     * request listeners and instance-id test
     */
    @Test
    public void req_listen_and_instid_test() throws Exception {
        String sname = "rl_and_iid_test", instid = "myinssst";
        DummyService.Iface client = ThriftClientBuilder.newBuilder(DummyService.Client.class)
                .withZookeeper(ZK).withServiceName(sname).withTimeout(5000).build();
        try {
            runTest(client, true, null, "-z", ZK, "-n", sname,
                    "-i", instid, "-l", TestRequestListener.ThrowingRequestListener.class.getName());
            assertTrue("req listener should have intercepted and thrown", false);
        } catch (Exception e) {
            assertNotNull(e.getMessage());
            assertTrue(e.getMessage(), e.getMessage().contains("INSTANCE ID IS " + instid));
        }
    }

    /**
     * test -v parameter
     */
    @Test
    public void service_version_test() throws Exception {
        String sname = "launched_test";
        final String serviceVersion = "version-5";
        DummyService.Iface client = ThriftClientBuilder.newBuilder(DummyService.Client.class)
                .withZookeeper(ZK).withServiceName(sname).withTimeout(5000).build();
        runTest(TestThriftServiceImpl.class, client, new Tester<DummyService.Iface>() {
            @Override
            public void test(Iface client) throws Exception {
                // verify version
                List<ServiceInstanceInfo> sis = ((LitelinksServiceClient) client).getServiceInstanceInfo();
                System.out.println("SIs: " + sis);
                assertTrue(sis != null && sis.size() > 0);
                assertEquals(serviceVersion, sis.get(0).getVersion());
            }
        }, true, null, null, "-z", ZK, "-n", sname, "-v", serviceVersion);
    }

    /**
     * test server read timeout
     */
    @Test
    public void read_timeout_test() throws Exception {
        String sname = "read_timeout_test";
        String[] jvmArgs = { "-Dlitelinks.server_read_timeout_secs=1" };
        final int port = 1089;
        final String PING = "#P";
        final TProtocolFactory tpf = new TCompactProtocol.Factory();
        final boolean[] gotFarEnough = new boolean[1];
        try {
            runTest(TestThriftServiceImpl.class, null, new Tester<DummyService.Iface>() {
                @Override
                public void test(Iface client) throws Exception {
                    try (TTransport tt = ClientTTransportFactory.NETTY
                            .openNewTransport("localhost", port, 2000L, false)) {
                        final TProtocol out = tpf.getProtocol(tt), in = tpf.getProtocol(tt);
                        TMessage msg = new TMessage(PING, TMessageType.CALL, ThreadLocalRandom.current().nextInt());
                        out.writeMessageBegin(msg);
                        out.getTransport().flush();
                        // wait for 1 sec mid-read, should trigger the timeout
                        Thread.sleep(1250L);
                        gotFarEnough[0] = true;
                        out.writeStructBegin(new TStruct());
                        out.writeFieldStop();
                        out.getTransport().flush();
                        out.writeStructEnd();
                        out.writeMessageEnd();
                        out.getTransport().flush();
                        TMessage msgin = in.readMessageBegin();
                        TApplicationException tae = null;
                        if (msgin.type == TMessageType.EXCEPTION) {
                            tae = TApplicationException.readFrom(in);
                        }
                        in.readMessageEnd();
                        if (tae != null && tae.getType() != TApplicationException.UNKNOWN_METHOD) {
                            throw tae;
                        }
                        if (msgin.seqid != msg.seqid || !PING.equals(msgin.name)) {
                            throw new TException("Unexpected ping response");
                        }
                    }
                }
            }, false, null, jvmArgs, "-n", sname, "-p", Integer.toString(port));
            assertTrue("Connection should have been closed (due to read timeout on server)",
                    false); // shouldn't reach here
        } catch (TConnectionClosedException tte) {
            // all good
        }
        assertTrue(gotFarEnough[0]);
    }

    /**
     * explicit port, protofac
     */
    @Test
    public void port_and_protofac_test() throws Exception {
        String sname = "launched_port_protofac_test";
        Class<? extends TProtocolFactory> protoFac = TJSONProtocol.Factory.class;
        int port = 1087;
        DummyService.Iface client1 = ThriftClientBuilder.newBuilder(DummyService.Client.class)
                .withZookeeper(ZK).withServiceName(sname).withTimeout(5000).build();
        // also static to verify port/protofac actually used
        DummyService.Iface client2 = ThriftClientBuilder.newBuilder(DummyService.Client.class)
                .withStaticServer("localhost", port, protoFac, DefaultThriftServer.DEFAULT_FRAMED, false)
                .withTimeout(5000).build();
        runTest(client1, true, null, "-z", ZK, "-n", sname, "-p", "" + port, "-f", protoFac.getName());
        runTest(client2, false, null, "-z", ZK, "-n", sname, "-p", "" + port, "-f", protoFac.getName());
    }

    /**
     * port via env var
     */
    @Test
    public void port_env_var_test() throws Exception {
        String sname = "launched_port_envvar_test";
        int port = 1088;
        DummyService.Iface client1 = ThriftClientBuilder.newBuilder(DummyService.Client.class)
                .withZookeeper(ZK).withServiceName(sname).withTimeout(5000).build();
        // also static to verify port actually used
        DummyService.Iface client2 = ThriftClientBuilder.newBuilder(DummyService.Client.class)
                .withStaticServer("localhost", port, null, DefaultThriftServer.DEFAULT_FRAMED, false)
                .withTimeout(5000).build();
        runTest(client1, true, Collections.singletonMap(LitelinksEnvVariableNames.PORT, "" + port), "-z", ZK, "-n",
                sname);
        runTest(client2, false, Collections.singletonMap(LitelinksEnvVariableNames.PORT, "" + port), "-z", ZK, "-n",
                sname);
    }

    /**
     * private domain functionality (for kubernetes - beta)
     */
    @Test
    public void private_endpoint_test() throws Exception {
        String sname = "private_endpoint_test";
        String sname2 = "private_endpoint_test2";
        int port = 1093, port2 = 1094;
        String usePrivProp = LitelinksSystemPropNames.PRIVATE_ENDPOINT;
        synchronized (PRIV_ENDPOINT_LOCK) {
            try {
                System.clearProperty(usePrivProp);
                DummyService.Iface client1 = ThriftClientBuilder.newBuilder(DummyService.Client.class)
                        .withZookeeper(ZK).withServiceName(sname).withTimeout(5000).build();

                System.setProperty(usePrivProp, "true");
                DummyService.Iface client2 = ThriftClientBuilder.newBuilder(DummyService.Client.class)
                        .withZookeeper(ZK).withServiceName(sname2).withTimeout(5000).build();

                Map<String, String> envvars = ImmutableMap.<String, String>builder()
                        .put(LitelinksEnvVariableNames.ADDRESS, "somethinginvalid")
                        .put(LitelinksEnvVariableNames.PRIVATE_ENDPOINT, "localhost:" + port).build();

                try {
                    // this should fail - should attempt to use invalid "external" address despite private
                    // one being there
                    runTest(client1, true, envvars, "-z", ZK, "-n", sname, "-p", "" + port);
                    assertTrue("test against invalid external address expected to to fail", false);
                } catch (TTransportException tte) {
                    System.out.println("Caught expected exception: " + tte);
                }
                envvars = ImmutableMap.<String, String>builder()
                        .put(LitelinksEnvVariableNames.ADDRESS, "somethinginvalid")
                        .put(LitelinksEnvVariableNames.PRIVATE_ENDPOINT, "localhost:" + port2).build();
                // this should work (use private address)
                runTest(client2, true, envvars, "-z", ZK, "-n", sname2, "-p", "" + port2);

            } finally {
                System.clearProperty(usePrivProp);
            }
        }
    }

    static final Object PRIV_ENDPOINT_LOCK = new Object();

    /**
     * private endpoint functionality (for kubernetes - beta)
     */
    @Test
    public void private_domains_test() throws Exception {
        String sname = "private_endpoint_test";
        String sname2 = "private_endpoint_test2";
        int port = 1093, port2 = 1094;
        String usePrivProp = LitelinksSystemPropNames.PRIVATE_ENDPOINT;
        String privateDomain = "mydomainid";
        synchronized (PRIV_ENDPOINT_LOCK) {
            try {
                System.clearProperty(usePrivProp);
                System.clearProperty(LitelinksSystemPropNames.PRIVATE_DOMAIN_ID);
                DummyService.Iface client1 = ThriftClientBuilder.newBuilder(DummyService.Client.class)
                        .withZookeeper(ZK).withServiceName(sname).withTimeout(5000).build();

                System.setProperty(usePrivProp, "true");
                System.setProperty(LitelinksSystemPropNames.PRIVATE_DOMAIN_ID, privateDomain);
                DummyService.Iface client2 = ThriftClientBuilder.newBuilder(DummyService.Client.class)
                        .withZookeeper(ZK).withServiceName(sname2).withTimeout(5000).build();

                Map<String, String> envvars = ImmutableMap.<String, String>builder()
                        .put(LitelinksEnvVariableNames.ADDRESS, "somethinginvalid")
                        .put(LitelinksEnvVariableNames.PRIVATE_ENDPOINT, "localhost:" + port).build();

                try {
                    // this should fail - should attempt to use invalid "external" address despite private
                    // one being there
                    runTest(client1, true, envvars, "-z", ZK, "-n", sname, "-p", "" + port);
                    assertTrue("test against invalid external address expected to to fail", false);
                } catch (TTransportException tte) {
                    System.out.println("Caught expected exception: " + tte);
                }

                envvars = ImmutableMap.<String, String>builder()
                        .put(LitelinksEnvVariableNames.ADDRESS, "somethinginvalid")
                        .put(LitelinksEnvVariableNames.PRIVATE_ENDPOINT, "localhost:" + port2)
                        .build();
                // this should work (no domain id)
                runTest(client2, true, envvars, "-z", ZK, "-n", sname2, "-p", "" + port2);
                envvars = ImmutableMap.<String, String>builder()
                        .put(LitelinksEnvVariableNames.ADDRESS, "somethinginvalid")
                        .put(LitelinksEnvVariableNames.PRIVATE_ENDPOINT, "localhost:" + port2)
                        .put(LitelinksEnvVariableNames.PRIVATE_DOMAIN_ID, privateDomain)
                        .build();
                // this should work (correct domain id)
                runTest(client2, true, envvars, "-z", ZK, "-n", sname2, "-p", "" + port2);
                envvars = ImmutableMap.<String, String>builder()
                        .put(LitelinksEnvVariableNames.ADDRESS, "somethinginvalid")
                        .put(LitelinksEnvVariableNames.PRIVATE_ENDPOINT,
                                "localhost:" + port2 + ";" + privateDomain)
                        .build();
                // this should work (correct domain id)
                runTest(client2, true, envvars, "-z", ZK, "-n", sname2, "-p", "" + port2);
                envvars = ImmutableMap.<String, String>builder()
                        .put(LitelinksEnvVariableNames.ADDRESS, "somethinginvalid")
                        .put(LitelinksEnvVariableNames.PRIVATE_ENDPOINT, "localhost:" + port2)
                        .put(LitelinksEnvVariableNames.PRIVATE_DOMAIN_ID, "differentdomain")
                        .build();
                try {
                    // this should fail - should attempt to use different domain id
                    runTest(client1, true, envvars, "-z", ZK, "-n", sname, "-p", "" + port);
                    assertTrue("test with invalid domain id expected to to fail", false);
                } catch (TTransportException tte) {
                    System.out.println("Caught expected exception: " + tte);
                }
            } finally {
                System.clearProperty(usePrivProp);
                System.clearProperty(LitelinksSystemPropNames.PRIVATE_DOMAIN_ID);
            }
        }
    }

    /**
     * implicit service name; zookeeper via env var
     */
    @Test
    public void zk_env_var_test() throws Exception {
        DummyService.Iface client = ThriftClientBuilder.newBuilder(DummyService.Client.class)
                .withZookeeper(ZK).withTimeout(5000).build();
        runTest(client, true, Collections.singletonMap(ZookeeperClient.ZK_CONN_STRING_ENV_VAR, ZK));
    }

    /**
     * empty service name on server side, should be treated as default
     */
    @Test
    public void empty_service_name_test() throws Exception {
        DummyService.Iface client = ThriftClientBuilder.newBuilder(DummyService.Client.class)
                .withZookeeper(ZK).withTimeout(5000).build();
        // note a space is used instead of an empty string here because java's ProcessBuilder doesn't appear
        // to handle empty args correctly
        runTest(client, true, null, "-z", ZK, "-n", " ");
    }

    /**
     * test thrift-only impl function (non ThriftService class)
     */
    @Test
    public void impl_only_test() throws Exception {
        String sname = "launched_impl_only_test";
        DummyService.Iface client = ThriftClientBuilder.newBuilder(DummyService.Client.class)
                .withZookeeper(ZK).withServiceName(sname).withTimeout(5000).build();
        runTest(SimpleThriftServiceImpl.class, client, true, null, "-z", ZK, "-n", sname);
        runTest(SimpleThriftServiceImpl.WithShutdown.class, client, true, null, "-z", ZK, "-n", sname);
    }


    public static void runTest(DummyService.Iface client, boolean availChecks,
            Map<String, String> envVars, String... testArgs) throws Exception {
        runTest(TestThriftServiceImpl.class, client, availChecks, envVars, testArgs);
    }

    public static void runTest(Class<?> svcClass, DummyService.Iface client, boolean availChecks,
            Map<String, String> envVars, String... testArgs) throws Exception {
        runTest(svcClass, client, availChecks, envVars, null, testArgs);
    }

    public static void runTest(Class<?> svcClass, DummyService.Iface client, boolean availChecks,
            Map<String, String> envVars, String[] jvmArgs, String... testArgs) throws Exception {
        runTest(svcClass, client, c -> {
            ((LitelinksServiceClient) c).testConnection();
            // run basic test with client
            LitelinksTests.basic_test(c, false);
        }, availChecks, envVars, jvmArgs, testArgs);
    }

    public static void runTest(Class<?> svcClass, DummyService.Iface client,
            Tester<DummyService.Iface> test, boolean availChecks,
            Map<String, String> envVars, String[] jvmArgs, String... testArgs) throws Exception {
        ProcessBuilder builder = buildSvcProcess(svcClass, envVars, jvmArgs, testArgs);
        // start the service
        final Process svcProc = builder.start();
        try {
            PrintWriter pw = new PrintWriter(svcProc.getOutputStream());
            // wait to become available
            if (availChecks) {
                assertTrue("Timeout waiting for service to become available",
                        ((LitelinksServiceClient) client).awaitAvailable(8000l));
            } else {
                Thread.sleep(3000l);
            }
            test.test(client);
            // send clean shutdown command to stdin
            pw.println("stop");
            pw.flush();
            // wait 6sec for proc to stop and be deregistered
            for (int i = 0; i <= 15 &&
                            availChecks && ((LitelinksServiceClient) client).isAvailable()
                            || isRunning(svcProc)
                    ; i++) {
                Thread.sleep(500l);
                if (i == 12) {
                    assertTrue("Service stop or deregistration timeout", false);
                }
            }
            // verify exit code
            assertEquals("unexpected nonzero exit code", 0, svcProc.exitValue());
        } finally {
            svcProc.destroy(); // should already be gone in success cases
        }
    }

    interface Tester<C> {
        void test(C client) throws Exception;
    }


    public static ProcessBuilder buildSvcProcess(Class<?> svcImplClass,
            Map<String, String> envVars, String[] jvmArgs, String... testArgs) {
        //TODO configure log4j for started proc to go to stdout
        List<String> args = new ArrayList<>(Arrays.asList(
                System.getProperty("java.home") + "/bin/java",
                "-cp", System.getProperty("java.class.path"),
                "-Xmx128M"));
        if (jvmArgs != null) {
            args.addAll(Arrays.asList(jvmArgs));
        }
        args.addAll(Arrays.asList(watsonSvcClass.getName(),
                "-s", svcImplClass.getName()));
        if (testArgs != null) {
            args.addAll(Arrays.asList(testArgs));
        }
        ProcessBuilder pb = new ProcessBuilder(args)
                .redirectErrorStream(true)
                .redirectOutput(Redirect.INHERIT);
        if (envVars != null) {
            pb.environment().putAll(envVars);
        }
        return pb;
    }

    public static boolean isRunning(Process proc) {
        try {
            proc.exitValue();
            return false;
        } catch (IllegalThreadStateException itse) {
            return true;
        }
    }


//	final BufferedReader br = new BufferedReader(new InputStreamReader(svcProc.getInputStream()));
//	new Thread() {
//		public void run() {
//			try {
//				String l;
//				while( (l=br.readLine()) != null) System.out.println("ServiceStdOut: "+l);
//			} catch(IOException e) {
//				e.printStackTrace();
//			}
//		};
//	}.start();

}
