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

import com.google.common.io.FileWriteMode;
import com.google.common.io.Files;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.Service.Listener;
import com.google.common.util.concurrent.Service.State;
import com.ibm.watson.kvutils.OrderedShutdownHooks;
import com.ibm.watson.kvutils.factory.KVUtilsFactory;
import com.ibm.watson.litelinks.InvalidThriftClassException;
import com.ibm.watson.litelinks.LitelinksEnvVariableNames;
import com.ibm.watson.litelinks.LitelinksSystemPropNames;
import com.ibm.watson.litelinks.ThreadPoolHelper;
import com.ibm.watson.litelinks.etcd.EtcdWatchedService;
import com.ibm.watson.litelinks.server.DefaultThriftServer.SSLMode;
import com.ibm.watson.zk.ZookeeperClient;
import io.netty.channel.epoll.Epoll;
import io.netty.handler.ssl.OpenSsl;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocolFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.ibm.watson.litelinks.ServiceRegistryTypes.*;

/**
 * This class is a standard launcher for service implementations
 */
public class LitelinksService {

    static {
        // Ensure these classes are initialized up-front to avoid native classloading deadlock (JDK bug?)
        Epoll.isAvailable();
        OpenSsl.isAvailable();
    }

    // port 0 means an available port will be automatically allocated (can be overridden with env var)
    public static final int DEFAULT_PORT = 0;

    public static final long DEFAULT_SHUTDOWN_TIMEOUT_MS = 30000L; // 30sec
    public static final long DEFAULT_SIG_SHUTDOWN_TIMEOUT_MS = 4000L; // 4secs

    // This is the Kubernetes default location
    public static final String DEFAULT_TERMINATION_MSG_PATH = "/dev/termination-log";
    public static final String KUBE_NATIVE_DIR = "/var/run/secrets/kubernetes.io";

    protected static final boolean cli = true;

    protected final Service theService;

    protected LitelinksService(Service theService) {
        this.theService = theService;
        theService.addListener(new Listener() {
            @Override
            public void terminated(State from) {
                shutdownLatch.countDown();
            }
            @Override
            public void failed(State from, Throwable failure) {
                serviceFailure = failure;
                shutdownLatch.countDown();
            }
        }, getServiceEventThreads());
    }

    public static void main(String[] args) {
        int rc;
        try {
            rc = launch(args);
        } catch (Throwable t) {
            t.printStackTrace(System.err);
            rc = 3;
        }
        if (!OrderedShutdownHooks.isInShutdown()) {
            // call System.exit() in case there are non-daemon
            // service impl threads
            System.exit(rc);
        }
    }

    public static int launch(String... args) throws Exception {
        // -s serviceClass -p port -n name
        ServiceDeploymentConfig config = buildConfig(args);
        if (config == null) {
            return badUsage();
        }
        String terminationMessage = null;
        try {
            final LitelinksService ws;
            try {
                ws = new LitelinksService(createService(config));
            } catch (Throwable t) {
                terminationMessage = terminationMessageFromThrowable(t);
                throw t;
            }
            String af = config.anchorFilename;
            if (af != null && !ws.startAnchorWatchThread(af)) {
                terminationMessage = "Anchor file does not exist: " + af;
                System.out.println(terminationMessage);
                return 2;
            }
            try {
                // add 3 seconds to startup time to ensure underlying service
                // timeout will trigger first (logs thread stacktrace etc)
                ws.run(getStartupTimeoutSecs(config) + 3);
            } catch (IllegalStateException ise) {
                System.out.println("service failed, exiting");
                throw ise;
            } finally {
                Throwable failure = ws.serviceFailure;
                if (failure != null) {
                    terminationMessage = ws.started? "Service failed post-startup" : "Service startup failed";
                    terminationMessage += ": " + terminationMessageFromThrowable(failure);
                }
            }
            return 0;
        } finally {
            writeTerminationMessage(terminationMessage);
        }
    }

    @SuppressWarnings("unchecked")
    // returns null for invalid usage of cmd line options
    static ServiceDeploymentConfig buildConfig(String... args) throws Exception {
        if (args.length % 2 != 0) {
            return null;
        }
        String sc = null, fc = null;
        ServiceDeploymentConfig config = new ServiceDeploymentConfig()
                .setReqListenerClasses(new LinkedList<Class<? extends RequestListener>>());
        for (int i = 0; i < args.length; i += 2) {
            try {
                String o = args[i], v = args[i + 1];
                if("-s".equals(o)) sc = v;
                else if("-n".equals(o)) config.setServiceName(v);
                else if("-p".equals(o)) config.setPort(Integer.parseInt(v));
                else if("-r".equals(o)) config.setPublicPort(Integer.parseInt(v));
                else if("-z".equals(o)) config.setZkConnString(v);
                else if("-f".equals(o)) fc = v;
                else if("-e".equals(o)) {
                    if("ssl".equals(v)) config.setSslMode(SSLMode.ENABLED);
                    else if("ssl-ca".equals(v)) config.setSslMode(SSLMode.CLIENT_AUTH);
                    else return null;
                }
                else if("-a".equals(o)) config.anchorFilename = v;
                else if("-t".equals(o)) config.setStartTimeoutSecs(Integer.parseInt(v));
                else if("-v".equals(o)) config.setServiceVersion(v);
                else if("-i".equals(o)) config.setInstanceId(v);
                else if("-h".equals(o)) config.setHealthProbePort(Integer.parseInt(v));
                else if("-l".equals(o)) {
                    try {
                        Class<?> clz = Class.forName(v);
                        if(!RequestListener.class.isAssignableFrom(clz)) {
                            throw new IllegalArgumentException("Provided listener class " +
                                    v + " must implement " + RequestListener.class.getName());
                        }
                        config.getReqListenerClasses().add((Class<? extends RequestListener>) clz);
                    } catch (ClassNotFoundException e) {
                        throw new ClassNotFoundException("Cannot find request listener class: " + v);
                    }
                } else {
                    return null;
                }
            } catch (NumberFormatException nfe) {
                return null;
            }
        }
        if (sc == null) {
            return null;
        }
        config.setServiceClass(Class.forName(sc));
        if (fc != null) {
            Class<?> fClz = Class.forName(fc);
            if (!TProtocolFactory.class.isAssignableFrom(fClz)) {
                throw new InvalidThriftClassException("Specified protocol factory class " + fClz
                    + " must implement " + TProtocolFactory.class.getName());
            }
            config.setProtoFactory((TProtocolFactory) fClz.newInstance());
        }
        if (config.getReqListenerClasses().isEmpty()) {
            config.setReqListenerClasses(null);
        }
        return config;
    }

    private static int badUsage() {
        System.err.println("bad arguments provided to litelinks launcher");
        System.err.println("usage: -s serviceClassName [-n serviceName] [-v serviceVersion]"
                           + " [-i instanceId] [-p servicePort] [-r publicPort]"
                           + " [-z zookeeperConnString] [-f protoFactoryClassName] [-a anchorFilePath]"
                           + " [-t startupTimeoutSecs] [-e (ssl|ssl-ca)]"
                           + " [-h httpHealthProbePort]"
                           + " { -l reqListenerClassName } * 0..n");
        return 1;
    }

    protected Thread startCLIThread() {
        Thread cliThread = new Thread() {
            { setDaemon(true); setName("LitelinksService CLI Thread"); }
            @Override public void run() {
                try (
                BufferedReader br = new BufferedReader(new InputStreamReader(System.in))) {
                    for (String in = ""; !"stop".equals(in); in = br.readLine()) {
                        if (in == null) return;
                    }
                    System.out.println("stopping");
                    LitelinksService.this.stop();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
        cliThread.start();
        return cliThread;
    }

    protected boolean startAnchorWatchThread(final String af) throws IOException {
        if (af == null) return false;
        final Path afPath = Paths.get(af);
        if (!afPath.toFile().exists()) {
            return false;
        }
        final WatchService watchService = FileSystems.getDefault().newWatchService();
        Path parent = afPath.getParent();
        if (parent == null) {
            parent = afPath.toAbsolutePath().getParent();
        }
        if (parent == null) {
            throw new IOException("Can't find parent dir for anchor file: " + af);
        }
        /*final WatchKey wKey =*/ parent.register(watchService, StandardWatchEventKinds.ENTRY_DELETE);
        if (!afPath.toFile().exists()) {
            try {
                watchService.close();
            } catch (IOException e) {}
            return false;
        }
        new Thread("LitelinksService Anchor Watcher") {
            { setDaemon(true); }

            @Override
            public void run() {
                try {
                    final Path afName = afPath.getFileName();
                    while (true) {
                        final WatchKey wk = watchService.take();
                        for (WatchEvent<?> ev : wk.pollEvents()) {
                            final Path changed = (Path) ev.context();
                            if (!afName.equals(changed)) {
                                continue;
                            }
                            System.out.println("Anchor file deleted, initiating shutdown: " + af);
                            LitelinksService.this.stop();
                        }
                        if (!wk.reset()) {
                            break;
                        }
                    }
                } catch (InterruptedException e) {
                    System.out.println("Anchor watch thread interrupted, initiating shutdown: " + af);
                    LitelinksService.this.stop();
                } finally {
                    try {
                        watchService.close();
                    } catch (IOException e) {}
                }
            }
        }.start();
        return true;
    }

    /**
     * Encapsulates configuration for service instance deployment
     */
    public static class ServiceDeploymentConfig {
        private Class<?> serviceClass;
        private TProcessor tProcessor;

        // of form "registry-type:conn-string"
        private String serviceRegistry;

        private String serviceName;
        private String serviceVersion;
        private int port = -1;
        private int publicPort = -1;
        private TProtocolFactory protoFactory;
        private SSLMode sslMode = SSLMode.NONE;
        private int startTimeoutSecs = -1;

        private int healthProbePort; // <= 0 means disabled

        private String instanceId;
        private List<Class<? extends RequestListener>> reqListenerClasses;

        private String anchorFilename;

        public ServiceDeploymentConfig() {}

        public ServiceDeploymentConfig(String serviceName) {
            this.serviceName = serviceName;
        }

        public ServiceDeploymentConfig(Class<?> serviceClass) {
            this.serviceClass = serviceClass;
        }

        /**
         * @param serviceClass must either be an implementation of {@link ThriftService}
         *                     or implement some thrift-generated service interface (XX.Iface)
         */
        public ServiceDeploymentConfig setServiceClass(Class<?> serviceClass) {
            this.serviceClass = serviceClass;
            return this;
        }
        public ServiceDeploymentConfig setTProcessor(TProcessor tProcessor) {
            this.tProcessor = tProcessor;
            return this;
        }
        /**
         * @param zkConnString env variable used if null
         * @deprecated use {@link #setServiceRegistry(String)} with "zookeeper:" prefix,
         * or the system props / env variables
         */
        @Deprecated
        public ServiceDeploymentConfig setZkConnString(String zkConnString) {
            this.serviceRegistry = zkConnString != null
                    ? ZOOKEEPER + ":" + zkConnString : null;
            return this;
        }
        /**
         * @param serviceRegistry of the form "type:config-string"
         */
        public ServiceDeploymentConfig setServiceRegistry(String serviceRegistry) {
            this.serviceRegistry = serviceRegistry;
            return this;
        }
        /**
         * @param serviceName derived from thrift service name if null or empty
         */
        public ServiceDeploymentConfig setServiceName(String serviceName) {
            this.serviceName = serviceName;
            return this;
        }
        /**
         * @param serviceVersion
         */
        public ServiceDeploymentConfig setServiceVersion(String serviceVersion) {
            this.serviceVersion = serviceVersion;
            return this;
        }
        /**
         * @param port 0 for auto-assign, -1 to use env variable/default
         */
        public ServiceDeploymentConfig setPort(int port) {
            this.port = port;
            return this;
        }
        /**
         * @param publicPort to be registered for use by clients;
         *                   0 to use same as listening port (recommended), -1 to use env variable/default
         */
        public ServiceDeploymentConfig setPublicPort(int publicPort) {
            this.publicPort = publicPort;
            return this;
        }
        /**
         * @param protoFactory default used if null
         */
        public ServiceDeploymentConfig setProtoFactory(TProtocolFactory protoFactory) {
            this.protoFactory = protoFactory;
            return this;
        }
        /**
         * @param sslMode
         */
        public ServiceDeploymentConfig setSslMode(SSLMode sslMode) {
            this.sslMode = sslMode;
            return this;
        }
        /**
         * @param startTimeoutSecs service startup/init timeout in secs, -1 for default (6mins)
         */
        public ServiceDeploymentConfig setStartTimeoutSecs(int startTimeoutSecs) {
            this.startTimeoutSecs = startTimeoutSecs;
            return this;
        }
        /**
         * @param instanceId auto-generated if null
         */
        public ServiceDeploymentConfig setInstanceId(String instanceId) {
            this.instanceId = instanceId;
            return this;
        }
        public ServiceDeploymentConfig setReqListenerClasses(
                List<Class<? extends RequestListener>> reqListenerClasses) {
            this.reqListenerClasses = reqListenerClasses;
            return this;
        }

        public ServiceDeploymentConfig setHealthProbePort(int healthProbePort) {
            this.healthProbePort = healthProbePort;
            return this;
        }

        public Class<?> getServiceClass() {
            return serviceClass;
        }
        public TProcessor getTProcessor() {
            return tProcessor;
        }
        /**
         * @deprecated use {@link #getServiceRegistry()}
         */
        @Deprecated
        public String getZkConnString() {
            return serviceRegistry != null && serviceRegistry.startsWith(ZOOKEEPER + ":")
                    ? serviceRegistry.substring(ZOOKEEPER.length() + 1) : null;
        }
        public String getServiceRegistry() {
            return serviceRegistry;
        }
        public String getServiceName() {
            return serviceName;
        }
        public int getPort() {
            return port;
        }
        public int getPublicPort() {
            return publicPort;
        }
        public TProtocolFactory getProtoFactory() {
            return protoFactory;
        }
        public SSLMode getSslMode() {
            return sslMode;
        }
        public String getServiceVersion() {
            return serviceVersion;
        }
        public int getStartTimeoutSecs() {
            return startTimeoutSecs;
        }
        public String getInstanceId() {
            return instanceId;
        }
        public List<Class<? extends RequestListener>> getReqListenerClasses() {
            return reqListenerClasses;
        }
        public int getHealthProbePort() {
            return healthProbePort;
        }
    }

    /**
     * @param serviceClass required, must either be an implementation of {@link ThriftService}
     *                     or implement some thrift-generated service interface (XX.Iface)
     */
    public static Service createService(Class<?> serviceClass) throws InvalidThriftClassException {
        return createService(new ServiceDeploymentConfig(serviceClass));
    }

    /**
     * @param serviceClass required, must either be an implementation of {@link ThriftService}
     *                     or implement some thrift-generated service interface (XX.Iface)
     * @param zkConnString env variable used if null
     * @param serviceName  derived from thrift service name if null or empty
     */
    public static Service createService(Class<?> serviceClass, String zkConnString, String serviceName)
            throws InvalidThriftClassException {
        ServiceDeploymentConfig config = new ServiceDeploymentConfig(serviceName)
                .setServiceClass(serviceClass).setZkConnString(zkConnString);
        return createService(config);
    }

    /**
     * @param serviceClass required, must either be an implementation of {@link ThriftService}
     *                     or implement some thrift-generated service interface (XX.Iface)
     * @param zkConnString env variable used if null
     * @param serviceName  derived from thrift service name if null or empty
     * @param port         0 for auto-assign, -1 to use env variable/default
     * @param protoFactory default used if null
     * @param sslMode
     */
    public static Service createService(Class<?> serviceClass, String zkConnString, String serviceName,
            int port, TProtocolFactory protoFactory, SSLMode sslMode) throws InvalidThriftClassException {
        ServiceDeploymentConfig config = new ServiceDeploymentConfig(serviceName)
                .setServiceClass(serviceClass).setZkConnString(zkConnString)
                .setPort(port).setProtoFactory(protoFactory).setSslMode(sslMode);
        return createService(config);
    }

    /**
     * @deprecated use {@link #createService(ServiceDeploymentConfig)}
     */
    @Deprecated
    public static Service createService(Class<?> serviceClass, String zkConnString, String serviceName,
            int port, int publicPort, TProtocolFactory protoFactory, SSLMode sslMode)
            throws InvalidThriftClassException {
        return createService(serviceClass, zkConnString, serviceName, port, publicPort, protoFactory, sslMode, -1);
    }

    /**
     * @deprecated use {@link #createService(ServiceDeploymentConfig)}
     */
    @Deprecated
    public static Service createService(Class<?> serviceClass, String zkConnString, String serviceName,
            int port, int publicPort, TProtocolFactory protoFactory, SSLMode sslMode, int startTimeoutSecs)
            throws InvalidThriftClassException {
        return createService(serviceClass, zkConnString, serviceName, null, port,
                publicPort, protoFactory, sslMode, startTimeoutSecs);
    }

    /**
     * @deprecated use {@link #createService(ServiceDeploymentConfig)}
     */
    @Deprecated
    public static Service createService(Class<?> serviceClass, String zkConnString, String serviceName,
            String serviceVersion, int port, int publicPort, TProtocolFactory protoFactory,
            SSLMode sslMode, int startTimeoutSecs) throws InvalidThriftClassException {
        ServiceDeploymentConfig config = new ServiceDeploymentConfig(serviceName)
                .setServiceClass(serviceClass).setZkConnString(zkConnString)
                .setServiceVersion(serviceVersion)
                .setPort(port).setPublicPort(publicPort)
                .setProtoFactory(protoFactory).setSslMode(sslMode)
                .setStartTimeoutSecs(startTimeoutSecs);
        return createService(config);
    }

    /**
     * @param config see setters of {@link ServiceDeploymentConfig}
     * @throws InvalidThriftClassException
     */
    @SuppressWarnings("unchecked")
    public static Service createService(ServiceDeploymentConfig config) throws InvalidThriftClassException {
        Class<?> serviceClass = config.serviceClass;
        if ((serviceClass == null) == (config.tProcessor == null)) {
            throw new IllegalArgumentException("must provide service class OR TProcessor");
        }
        int port = config.port != -1 ? config.port : getDefaultPort(LitelinksEnvVariableNames.PORT);
        int publicPort =
                config.publicPort != -1 ? config.publicPort : getDefaultPort(LitelinksEnvVariableNames.PUBLIC_PORT);
        String serviceName =
                config.serviceName == null || !config.serviceName.trim().isEmpty()? config.serviceName : null;
        int startTimeoutSecs = getStartupTimeoutSecs(config);

        // instantiate any provided RequestListeners
        RequestListener[] reqListeners = null;
        if (config.reqListenerClasses != null && !config.reqListenerClasses.isEmpty()) {
            reqListeners = new RequestListener[config.reqListenerClasses.size()];
            int i = 0;
            for (Class<? extends RequestListener> clz : config.reqListenerClasses) {
                try {
                    reqListeners[i++] = clz.newInstance();
                } catch (IllegalAccessException | InstantiationException e) {
                    throw new IllegalArgumentException(
                            "Problem instantiating provided request listener: " + clz.getName(), e);
                }
            }
        }

        final InetSocketAddress listenAddress = new InetSocketAddress(port);
        final DefaultThriftServer tservice;
        if (serviceClass != null) {
            if (ThriftService.class.isAssignableFrom(serviceClass)) {
                try {
                    serviceClass.getConstructor();
                } catch (NoSuchMethodException e) {
                    throw new InvalidThriftClassException(ThriftService.class.getSimpleName()
                        + " implementation must declare a no-arg (or default) constructor");
                }
                tservice = new DefaultThriftServer((Class<? extends ThriftService>) serviceClass, listenAddress,
                        config.protoFactory, config.sslMode, startTimeoutSecs, reqListeners);
            } else {
                ThriftService wrapper = new AdapterThriftService(serviceClass); // throws InvalidThriftClassException
                tservice = new DefaultThriftServer(wrapper, listenAddress, config.protoFactory, config.sslMode,
                        startTimeoutSecs, reqListeners);
            }
        } else {
            tservice = new DefaultThriftServer(config.tProcessor, listenAddress, config.protoFactory, config.sslMode,
                    startTimeoutSecs, reqListeners);
        }

        String registryString = getRegistryString(config), type, connString;
        if (registryString == null) {
            // for backwards compatibility next look for ZOOKEEPER env var
            String zkString = System.getenv(ZookeeperClient.ZK_CONN_STRING_ENV_VAR);
            if (zkString != null) {
                registryString = ZOOKEEPER + ":" + zkString.trim();
            } else {
                // as a final fallback, use KV_STORE env var if present
                registryString = System.getProperty(KVUtilsFactory.KV_STORE_EV);
                if (registryString == null) {
                    registryString = System.getenv(KVUtilsFactory.KV_STORE_EV);
                    if (registryString == null) {
                        registryString = NONE + ":";
                    }
                }
            }
        }

        System.out.println("using service registry string: " + registryString);
        int delim = registryString.indexOf(':');
        if (delim < 0) {
            throw new IllegalArgumentException("Invalid service registry string: " + registryString);
        }
        type = registryString.substring(0, delim);
        connString = registryString.substring(delim + 1);

        switch (type) {
        case ZOOKEEPER:
            return new ZookeeperWatchedService(tservice, connString, serviceName,
                    config.serviceVersion, config.instanceId, publicPort, config.healthProbePort);
        case ETCD:
            return new EtcdWatchedService(tservice, connString, serviceName,
                    config.serviceVersion, config.instanceId, publicPort, config.healthProbePort);
        case NONE:
            return new UnWatchedService(tservice, serviceName,
                    config.serviceVersion, config.instanceId, publicPort, config.healthProbePort);
        default:
            throw new IllegalArgumentException("Unrecognized service registry type: " + type);
        }
    }

    static String getRegistryString(ServiceDeploymentConfig config) {
        if (config.serviceRegistry != null) {
            return config.serviceRegistry;
        }
        String registryParam = System.getProperty(LitelinksSystemPropNames.SERVER_REGISTRY);
        return registryParam != null? registryParam
                : System.getenv(LitelinksEnvVariableNames.SERVER_REGISTRY);
    }

    static int getDefaultPort(String envVar) throws NumberFormatException {
        //get the port from the env variable or fallback to the default
        String port = System.getenv(envVar);
        try {
            return port != null? Integer.parseInt(port) : DEFAULT_PORT;
        } catch (NumberFormatException nfe) {
            throw new NumberFormatException("The " + envVar + " environment variable is not a number: " + port);
        }
    }

    protected final CountDownLatch shutdownLatch = new CountDownLatch(1);
    protected volatile Throwable serviceFailure;
    protected volatile boolean started;

    public void run(int startupTimeoutSecs) {
        if (theService == null) {
            return;
        }
        ServiceShutdownHook.registerForShutdown(this);
        theService.startAsync();
        try {
            if (cli) {
                startCLIThread();
            }
            System.out.println("service starting" + (cli? ", type \"stop\" to stop" : ""));

            try {
                theService.awaitRunning(startupTimeoutSecs, TimeUnit.SECONDS);
                System.out.println("service started");
                started = true;
            } catch (IllegalStateException ise) {
                final State ss = theService.state();
                // this could be 'valid' if the service is stopped while starting
                if (ss != State.STOPPING && ss != State.TERMINATED) {
                    throw ise;
                }
            } catch (TimeoutException e) {
                if (serviceFailure == null) {
                    serviceFailure = e;
                }
                throw new IllegalStateException("Timed out waiting for the service to start", e);
            }

            /* This latch is tripped when the service should exit, i.e. upon failure
             * or after a stop request and either graceful shutdown completes or the
             * appropriate shutdown timeout expires. The initial stop request could
             * come from stdin, anchor file deletion, or a process termination signal
             * (see LitelinksShutdownHook class)
             */
            try {
                shutdownLatch.await();
                Throwable failure = serviceFailure;
                if (failure == null) {
                    System.out.println("service stopped");
                } else {
                    System.err.println("service failed");
                    failure.printStackTrace();
                }
            } catch (InterruptedException e) {
                System.out.println("Main thread interrupted, exiting");
                Thread.currentThread().interrupt();
            }
        } finally {
            ServiceShutdownHook.removeShutdownRegistration(this);
        }
    }

    public void stop() {
        stopWithDeadline(getShutdownTimeoutMs());
    }

    public LitelinksService stopWithDeadline(long timeoutMs) {
        if (theService != null) {
            theService.stopAsync();
            if (!serviceIsFinished()) {
                getServiceEventThreads().schedule(() -> {
                    if (!serviceIsFinished()) {
                        if (shutdownLatch.getCount() > 0) {
                            System.out.println("Service shutdown timed out, aborting");
                            shutdownLatch.countDown();
                        }
                    }
                }, timeoutMs, TimeUnit.MILLISECONDS);
            }
        }
        return this;
    }

    public boolean awaitStopped(long millisecs) throws InterruptedException {
        if (theService == null) {
            return true;
        }
        return shutdownLatch.await(millisecs, TimeUnit.MILLISECONDS);
    }

    protected boolean serviceIsFinished() {
        if (theService == null) {
            return true;
        }
        State state = theService.state();
        return state == State.TERMINATED || state == State.FAILED;
    }

    private static String terminationMessageFromThrowable(Throwable t) {
        if (t == null) {
            return null;
        }
        String prefix = t.getClass() == Exception.class || t.getClass() == Throwable.class
                ? "" : t.getClass().getSimpleName() + ": ";
        String message = t.getLocalizedMessage();
        return prefix + (message != null && !message.isEmpty()? message : "See logs for details");
    }

    private static void writeTerminationMessage(String message) {
        if (message == null) {
            return;
        }
        String path = System.getenv(LitelinksEnvVariableNames.TERMINATION_MSG_PATH);
        if (path == null) {
            if (!new File(KUBE_NATIVE_DIR).exists()) {
                // Don't write termination message if path is not explicitly provided
                // via env var and we aren't running in Kubernetes
                return;
            }
            path = DEFAULT_TERMINATION_MSG_PATH;
        }

        File file = new File(path);
        try {
            Files.asCharSink(file, StandardCharsets.UTF_8, FileWriteMode.APPEND).write(message + "\n");
            System.out.println("Wrote termination message to " + path + ": \"" + message + "\"");
        } catch (IOException ioe) {
            System.err.println("Exception writing termination message to " + path + ": " + ioe);
        }
    }


    protected static int getStartupTimeoutSecs(ServiceDeploymentConfig config) {
        return config.startTimeoutSecs == -1? DefaultThriftServer.DEFAULT_STARTUP_TIMEOUT_SECS :
                config.startTimeoutSecs;
    }

    protected static long getShutdownTimeoutMs() {
        String sst = System.getProperty(LitelinksSystemPropNames.SERVER_SHUTDOWN_TIMEOUT);
        if (sst == null) {
            return DEFAULT_SHUTDOWN_TIMEOUT_MS;
        }
        return Long.parseLong(sst.split(",")[0]);
    }

    public static long getSignalShutdownTimeoutMs() {
        String sst = System.getProperty(LitelinksSystemPropNames.SERVER_SHUTDOWN_TIMEOUT);
        if (sst == null) {
            return DEFAULT_SIG_SHUTDOWN_TIMEOUT_MS;
        }
        String[] parts = sst.split(",");
        return Long.parseLong(parts.length > 1? parts[1] : parts[0]);
    }

    protected static long getMaxShutdownTimeoutMs() {
        return Math.max(getShutdownTimeoutMs(), getSignalShutdownTimeoutMs());
    }

    // thread pool shared by service lifecycle components
    private static volatile ScheduledExecutorService serviceEventThreads;

    static ScheduledExecutorService getServiceEventThreads() {
        ScheduledExecutorService ses = serviceEventThreads;
        if (ses == null) {
            synchronized (DefaultThriftServer.class) {
                if (serviceEventThreads == null) {
                    final ThreadFactory tf = ThreadPoolHelper.threadFactory("ll-svc-events-%d");
                    serviceEventThreads = new ScheduledThreadPoolExecutor(2, tf) {
                        // delegate immediate tasks to regular ThreadPool which can grow
                        private final ExecutorService immediate =
                                new ThreadPoolExecutor(3, Integer.MAX_VALUE,
                                        5L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(), tf);
                        @Override
                        public void execute(Runnable command) {
                            immediate.execute(command);
                        }
                        @Override
                        public void shutdown() {
                            super.shutdown();
                            immediate.shutdown();
                        }
                        @Override
                        public List<Runnable> shutdownNow() {
                            List<Runnable> list = new ArrayList<>(super.shutdownNow());
                            list.addAll(immediate.shutdownNow());
                            return list;
                        }
                    };

                    OrderedShutdownHooks.addHook(5, serviceEventThreads::shutdown);
                }
                ses = serviceEventThreads;
            }
        }
        return ses;
    }
}
