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
package com.ibm.watson.litelinks.client;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.ibm.watson.kvutils.OrderedShutdownHooks;
import com.ibm.watson.litelinks.InvalidThriftClassException;
import com.ibm.watson.litelinks.LitelinksEnvVariableNames;
import com.ibm.watson.litelinks.LitelinksExceptions;
import com.ibm.watson.litelinks.LitelinksSystemPropNames;
import com.ibm.watson.litelinks.LitelinksTProtoExtension;
import com.ibm.watson.litelinks.LitelinksTProtoExtension.InterceptTOutProto;
import com.ibm.watson.litelinks.MethodInfo;
import com.ibm.watson.litelinks.NettyTTransport;
import com.ibm.watson.litelinks.SSLHelper;
import com.ibm.watson.litelinks.SSLHelper.SSLParams;
import com.ibm.watson.litelinks.ThriftConnProp;
import com.ibm.watson.litelinks.client.LitelinksServiceClient.ServiceInstanceInfo;
import com.ibm.watson.litelinks.client.ServiceInstance.ServiceInstanceConfig;
import com.ibm.watson.litelinks.client.ServiceInstanceCache.Balancers;
import com.ibm.watson.litelinks.client.ServiceInstanceCache.ListenerWithExecutor;
import com.ibm.watson.litelinks.client.ServiceInstanceCache.ServiceClientManager;
import com.ibm.watson.litelinks.client.ServiceRegistryClient.ServiceWatcher;
import io.netty.handler.ssl.SslContext;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.TServiceClientFactory;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.net.SocketException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.ibm.watson.litelinks.LitelinksTProtoExtension.getOptimizedTProtoFactory;

public class TServiceClientManager<C extends TServiceClient>
        implements ServiceClientManager<C>, AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(TServiceClientManager.class);

    static final String THRIFT_CLIENT_IFACE_NAME = "Iface";
    static final String THRIFT_CLIENT_ASYNC_IFACE_NAME = "AsyncIface";

    public static final long DISCOVERY_INIT_TIMEOUT = 20000l; //20sec TBD

    public static final TProtocolFactory DEFAULT_TPROTOFAC = new TCompactProtocol.Factory();
    //    == com.ibm.litelinks.server.DefaultThriftServer.DEFAULT_TPROTOFAC

    public static final LoadBalancingPolicy DEFAULT_LB_POLICY = LoadBalancingPolicy.BALANCED;

    public static final boolean DELAY_CLOSING =
            !"false".equals(System.getProperty(LitelinksSystemPropNames.DELAY_CLIENT_CLOSING));
    public static final boolean VERIFY_UNAVAILABLE =
            !"false".equals(System.getProperty(LitelinksSystemPropNames.VERIFY_UNAVAIL));

    static {
        logger.info(LitelinksSystemPropNames.DELAY_CLIENT_CLOSING + "=" + DELAY_CLOSING);
        logger.info(LitelinksSystemPropNames.VERIFY_UNAVAIL + "=" + VERIFY_UNAVAILABLE);
    }

    private final ServiceKey serviceKey;
    private final String serviceName;
    private final ServiceInstanceCache<C> serviceCache;
    private final ServiceWatcher serviceWatcher;

    private final AtomicInteger refCount = new AtomicInteger();
    private long closeTime = Long.MAX_VALUE;

    // this is for compatibility validation when connecting to servers;
    // may later incorporate interface version
    private final String serviceClassName;
    private final Class<?> serviceIface;

    private Set<String> ifaceMethods; // set lazily

    private final ClientTTransportFactory transFactory = ClientTTransportFactory.NETTY; //TODO TBD
    private final TServiceClientFactory<C> clientFactory;

    // reuse same exception object - avoids construction overhead
    final Exception ServiceUnavailableException;

    // clientpools per servicename/zk pair
    private static final LoadingCache<ServiceKey, TServiceClientManager<?>> clientMgrCache
            = CacheBuilder.newBuilder().removalListener(new RemovalListener<ServiceKey, TServiceClientManager<?>>() {
        @Override
        public void onRemoval(RemovalNotification<ServiceKey, TServiceClientManager<?>> notif) {
            notif.getValue().doClose();
        }
    }).build(new CacheLoader<ServiceKey, TServiceClientManager<?>>() {
        @Override
        public TServiceClientManager<?> load(ServiceKey key) throws Exception {
            for (Class<?> clz : key.serviceClass.getDeclaredClasses()) {
                if (TServiceClientFactory.class.isAssignableFrom(clz)) {
                    TServiceClientFactory<?> factory;
                    try {
                        // assert "Factory".equals(clz.getSimpleName());
                        factory = (TServiceClientFactory<?>) clz.newInstance();
                    } catch (IllegalAccessException | InstantiationException | ClassCastException e) {
                        throw new InvalidThriftClassException(e);
                    }
                    String serviceClassName, serviceName;
                    if (key.mplexerName != null) {
                        serviceName = key.mplexerName;
                        serviceClassName = ThriftConnProp.MULTIPLEX_CLASS;
                        factory = new TMultiplexClientFactory<>(factory, key.name);
                    } else {
                        serviceName = key.name;
                        serviceClassName = TGenericServiceClient.class.equals(key.serviceClass)
                                ? null : key.serviceClass.getDeclaringClass().getName();
                    }

                    // below throws service discovery init exceptions
                    return new TServiceClientManager<>(key.svcRegistry, serviceName, serviceClassName, factory, key);
                }
            }
            throw new InvalidThriftClassException("Service Client Factory not found");
        }
    });


    static {
        // this will cleanly disconnect connections to services
        OrderedShutdownHooks.addHook(50, clientMgrCache::invalidateAll);
    }

    public static <C extends TServiceClient> TServiceClientManager<C> get(Class<C> clientClass) throws Exception {
        return get(null, null, null, clientClass);
    }

    private static final int MAX_REINITS = 4;

    @SuppressWarnings("unchecked")
    public static <C extends TServiceClient> TServiceClientManager<C> get(final ServiceRegistryClient svcReg,
            String serviceName, String mplexerName, Class<C> clientClass) throws Exception {
        if (clientClass != null) {
            Class<?> encClass = clientClass.getDeclaringClass();
            if (encClass == null) {
                throw new InvalidThriftClassException("client class must be enclosed in service class");
            }
            if (serviceName == null) {
                serviceName = encClass.getName(); // serviceClassName
            }
        } else {
            // anonymous client case
            if (serviceName == null) {
                throw new IllegalArgumentException("Must provide service name");
            }
            clientClass = (Class<C>) TGenericServiceClient.class;
        }
        final ServiceKey serviceKey = new ServiceKey(serviceName, mplexerName, clientClass, svcReg);
        for (int i = 0; i < MAX_REINITS; i++) {
            try {
                TServiceClientManager<C> cm = (TServiceClientManager<C>) clientMgrCache.get(serviceKey);
                if (cm.isValid()) {
                    cm.incRefs();
                    return cm;
                }
                clientMgrCache.asMap().remove(serviceKey, cm); // will be re-initialized
                logger.info("ClientManager for service " + serviceKey.name + " invalid, re-initializing");
            } catch (ExecutionException e) {
                throw (Exception) e.getCause();
            } finally {
                processCloseQueue();
            }
        }
        throw new Exception("client init error");
    }

    // checked exceptions are zookeeper-related
    @SuppressWarnings("resource")
    TServiceClientManager(ServiceRegistryClient svcReg, String serviceName,
            String serviceClassName, final TServiceClientFactory<C> factory, ServiceKey svcKey) throws Exception {
        ServiceInstanceCache<C> svcCache = null;
        ServiceWatcher svcWatcher = null;
        try {
            this.serviceKey = svcKey;
            this.serviceName = serviceName;
            this.serviceClassName = serviceClassName;
            this.serviceIface = serviceClassName != null && !ThriftConnProp.MULTIPLEX_CLASS.equals(serviceClassName)
                    ? getIfaceFromSvcClass(Class.forName(serviceClassName)) : null;
            this.ServiceUnavailableException = LitelinksExceptions.eraseStackTrace(
                    new ServiceUnavailableException(serviceName));

            this.clientFactory = factory;
            this.serviceCache = svcCache = new ServiceInstanceCache<C>(serviceName, /*DEFAULT_LB_POLICY,*/ this);

            sendMDC = getThreadContextProps()[1];

            logger.info("Creating client for service " + serviceName + "; send log_mdc=" + sendMDC);

            this.serviceWatcher = svcWatcher = svcReg.newServiceWatcher(serviceName);
            svcWatcher.start(serviceCache, DISCOVERY_INIT_TIMEOUT);
            svcCache = null;
            svcWatcher = null;
        } finally {
            // make sure resources are closed if we aren't successful
            if (svcWatcher != null) {
                try {
                    svcWatcher.close();
                } catch (RuntimeException e) {
                    logger.warn("Exception closing ServiceWatcher (service=" + serviceName + ")", e);
                }
            }
            if (svcCache != null) {
                svcCache.close(); // disconnects conn pools, etc
            }
        }
    }

    protected static Class<?> getIfaceFromSvcClass(Class<?> svcClass) {
        for (Class<?> inner : svcClass.getDeclaredClasses()) {
            if (inner.getSimpleName().equals(THRIFT_CLIENT_IFACE_NAME)) {
                return inner;
            }
        }
        return null;
    }

    private Set<String> getIfaceMethods() {
        if (serviceIface == null) {
            return Collections.emptySet();
        }
        if (ifaceMethods != null) {
            return ifaceMethods;
        }
        ImmutableSet.Builder<String> isb = ImmutableSet.builder();
        for (Method m : serviceIface.getMethods()) {
            isb.add(m.getName());
        }
        return ifaceMethods = isb.build();
    }

    /**
     * borrowed clients must be released via a single invocation
     * of {@link ServiceInstance.PooledClient#releaseClient(ServiceInstance.FailType)}
     */
    public ServiceInstance<C>.PooledClient borrowClient(Balancers balancers,
            String method, Object[] args) throws Exception {
        ServiceInstance<C>.PooledClient pc;
        Set<ServiceInstance<C>> seen = null;
        while (true) {
            ServiceInstance<C> si = serviceCache.getNextServiceInstance(balancers, method, args);
            if (si == null && balancers.inclusive && !confirmUnavailable()) {
                si = serviceCache.getNextServiceInstance(balancers, method, args);
            }
            if (si == null) {
                throw ServiceUnavailableException; // no service instances available
            }
            if (si == ServiceInstanceCache.ALL_FAILING) {
                throw new ServiceUnavailableException("all instances are failing: " + serviceName);
            }
            try {
                pc = si.borrowClient();
            } catch (IllegalStateException ise) {
                continue; // service inactive
            } catch (TTransportException | SocketException | NoSuchElementException te) {
                // NoSuchElementException is thrown if the pool's validation of the
                // connection fails, which means that the connection's transport is not open.
                if (seen == null) {
                    seen = new HashSet<>();
                }
                // don't try the same SI more than twice
                // (won't happen before activeList has been exhausted)
                if (!seen.add(si)) {
                    throw te;
                }
                continue;
            }
            return pc;
        }
    }

    public void addListeners(Collection<ListenerWithExecutor> listeners) {
        serviceCache.addListeners(listeners);
    }

    public void removeListeners(Collection<ListenerWithExecutor> listeners) {
        serviceCache.removeListeners(listeners);
    }

    /**
     * for instrumentation only
     */
    public List<ServiceInstanceInfo> getServiceInstanceInfo() {
        return serviceCache.getServiceInstanceInfo();
    }

    public boolean awaitAvailable(long timeoutMillis) throws InterruptedException {
        boolean avail = serviceCache.awaitAvailable(timeoutMillis);
        return avail || !confirmUnavailable();
    }

    private boolean confirmUnavailable() {
        return !VERIFY_UNAVAILABLE || serviceWatcher.confirmUnavailable();
    }

    public boolean isValid() {
        return serviceWatcher.isValid() && !serviceCache.isClosed();
    }

    @Override
    public void close() {
        int nrc;
        if ((nrc = refCount.decrementAndGet()) == 0) {
            synchronized (this) {
                if (DELAY_CLOSING) {
                    // When all clients using this pool have been closed, don't
                    // close it immediately in case another client is created for
                    // the same service in the near future. Instead, queue it to
                    // be closed a bit later
                    if (closeTime < Long.MAX_VALUE) {
                        closeQueue.remove(this);
                    }
                    closeTime = System.currentTimeMillis();
                    if (logger.isDebugEnabled()) {
                        logger.debug("Enqueueing TSCM " + this
                                     + " (service=" + serviceName + ") with closetime: " +
                                     closeTime);
                    }
                    closeQueue.add(this);
                } else {
                    // gets closed in the cache removal event notification
                    clientMgrCache.asMap().remove(serviceKey, this);
                }
            }
        } else if (logger.isDebugEnabled()) {
            logger.debug("close() called for " + this
                         + " (service=" + serviceName + "), new refcount=" + nrc);
        }
    }

    private void incRefs() {
        refCount.incrementAndGet();
    }

    private void doClose() {
        if (logger.isDebugEnabled()) {
            logger.debug("Closing TSCM: " + this + " (service=" + serviceName + ")");
        }
        try {
            serviceWatcher.close();
        } catch (RuntimeException e) {
            logger.warn("Exception closing ServiceWatcher (service=" + serviceName + ")", e);
        }
        serviceCache.close(); // disconnects conn pools, etc
    }


    private static final BlockingQueue<TServiceClientManager<?>> closeQueue
            = DELAY_CLOSING ? new PriorityBlockingQueue<>(11, new Comparator<TServiceClientManager<?>>() {
        @Override
        public int compare(TServiceClientManager<?> cm1,
                TServiceClientManager<?> cm2) {
            return Long.compare(cm1.closeTime, cm2.closeTime);
        }
    }) : null;

    /* Here close pending CM instances based on when the corresponding
     * client was closed and the number there currently are in the cache.
     * Can't use a DelayQueue because of the second criteria.
     */
    private static void processCloseQueue() {
        if (closeQueue == null) {
            return;
        }
        int queueSize = closeQueue.size();
        if (queueSize > 0) {
            long activeSize = clientMgrCache.size();
            if (activeSize == 0) {
                return;
            }
            long cutoff;
            while (activeSize > 0) {
                TServiceClientManager<?> cm = closeQueue.peek();
                if (cm == null || cm.closeTime > (cutoff = cutoff(activeSize))) {
                    return;
                }
                cm = closeQueue.poll();
                if (cm.refCount.get() > 0) {
                    continue; // in use again, discard entry
                }
                if (cm.closeTime > cutoff) {
                    closeQueue.add(cm); // put back
                    return;
                }
                if (clientMgrCache.asMap().remove(cm.serviceKey, cm)) {
                    cutoff = cutoff(--activeSize);
                }
            }
        }
    }

    private static long cutoff(long size) {
        return System.currentTimeMillis() - 691200000l / size;
    }

    private static final int MD_PREFIX_LEN = ThriftConnProp.APP_METADATA_PREFIX.length();
    private static final int MI_PREFIX_LEN = ThriftConnProp.METH_INFO_PREFIX.length();

    @Override
    public ServiceInstanceConfig<C> getInstanceConfig(String hostname, int port,
            long registrationTime, String version, Map<Object, Object> connConfig) throws Exception {
        final String sc = (String) connConfig.get(ThriftConnProp.SERVICE_CLASS);
        if (sc != null && serviceClassName != null && !sc.equals(serviceClassName)) {
            Class<?> otherIface = null;
            if (serviceIface != null) {
                try {
                    otherIface = getIfaceFromSvcClass(Class.forName(sc));
                } catch (ClassNotFoundException cnfe) {}
            }
            if (otherIface == null || !serviceIface.isAssignableFrom(otherIface)) {
                throw new Exception("service class mismatch: expecting " + serviceClassName
                                    + " but server is " + sc); //TODO maybe custom exception type
            }
        }
        if (hostname == null) {
            throw new Exception("instance host address missing");
        }
        if (!HOSTNAME_PATT.matcher(hostname).matches()) {
            throw new Exception("invalid instance address \"" + hostname + "\"");
        }

        ImmutableMap.Builder<String, String> metaImb = null;
        ImmutableMap.Builder<String, MethodInfo> miImb = null;
        if (connConfig != null) {
            String val = (String) connConfig.remove(ThriftConnProp.METH_INFO_PREFIX + MethodInfo.DEFAULT);
            if (serviceIface != null && val != null) {
                try {
                    (miImb = ImmutableMap.builder()).put(MethodInfo.DEFAULT, MethodInfo.deserialize(val));
                } catch (Exception e) {
                    logger.warn("Error parsing registered default method info >" + val + "<", e);
                }
            }
            for (Iterator<Entry<Object, Object>> it = connConfig.entrySet().iterator(); it.hasNext(); ) {
                Entry<Object, Object> ent = it.next();
                if (ent.getKey() == null || ent.getValue() == null) {
                    continue;
                }
                String key = ent.getKey().toString();
                val = ent.getValue().toString();
                if (key.startsWith(ThriftConnProp.APP_METADATA_PREFIX)) {
                    it.remove(); // remove from connConfig
                    if (metaImb == null) {
                        metaImb = ImmutableMap.builder();
                    }
                    metaImb.put(key.substring(MD_PREFIX_LEN), val);
                } else if (key.startsWith(ThriftConnProp.METH_INFO_PREFIX)) {
                    it.remove();
                    if (serviceIface != null) {
                        String methName = key.substring(MI_PREFIX_LEN);
                        if (!getIfaceMethods().contains(methName)) {
                            logger.warn("Discovered info for unrecognized method: " + methName);
                        } else {
                            if (miImb == null) {
                                miImb = ImmutableMap.builder();
                            }
                            try {
                                miImb.put(methName, MethodInfo.deserialize(val));
                            } catch (Exception e) {
                                logger.warn("Error parsing registered info for method "
                                            + methName + " >" + val + "<", e);
                            }
                        }
                    }
                }
            }
        }
        Map<String, String> metadata = metaImb != null ? metaImb.build()
                : Collections.emptyMap();
        Map<String, MethodInfo> methodInfos = miImb != null ? miImb.build()
                : Collections.emptyMap();
        return new ThriftInstanceConfig(hostname, port, version, registrationTime,
                connConfig, metadata, methodInfos);
    }

    @Override
    public C createClient(ServiceInstanceConfig<C> config, long timeoutMillis) throws TTransportException {
        if (!(config instanceof TServiceClientManager.ThriftInstanceConfig)) {
            throw new IllegalStateException("Invalid type of service config: "
                                            + config != null ? config.getClass().toString() : "null");
        }
        return ((ThriftInstanceConfig) config).createClient(timeoutMillis);
    }

    @Override
    public boolean isTransportException(Throwable t) {
        if (t instanceof TTransportException
            || t instanceof TProtocolException
                || t instanceof SocketException) return true;
        if(!(t instanceof TApplicationException)) return false;
        int type = ((TApplicationException) t).getType();
        return type == TApplicationException.PROTOCOL_ERROR
               || type == TApplicationException.INVALID_PROTOCOL
               || type == TApplicationException.UNSUPPORTED_CLIENT_TYPE;
    }

    @Override
    public boolean isValid(C client) {
        try {
            return client.getOutputProtocol().getTransport().isOpen();
        } catch (NullPointerException e) {
            if (client == null) {
                logger.warn("Invalid client (null)");
            } else if (client.getOutputProtocol() == null) {
                logger.warn("Invalid client (null output proto)");
            } else if (client.getOutputProtocol().getTransport() == null) {
                logger.warn("Invalid client (null transport)");
            } else {
                logger.warn("Invalid client", e);
            }
            return false;
        }
    }

    @Override
    public void close(C client) {
        client.getOutputProtocol().getTransport().close();
    }

    private static final long MAX_PING_TIMEOUT = 3000L; // 3sec

    @Override
    public void testConnection(C client, long timeoutMs) throws TException {
        timeoutMs = Math.min(timeoutMs, MAX_PING_TIMEOUT);
        TProtocol in = client.getInputProtocol(), out = client.getOutputProtocol();
        TTransport tt = out.getTransport();
        if (tt instanceof NettyTTransport) {
            ((NettyTTransport) tt).startIOTimer(timeoutMs);
        } else if (tt instanceof TSocket) {
            ((TSocket) tt).setTimeout((int) timeoutMs);
        }
        LitelinksTProtoExtension.ping(in, out);
    }

    //TODO for now this is 'global' for all clients in the JVM;
    // could make configurable per client
    public final boolean sendMDC;

    private static boolean[] getThreadContextProps() {
        boolean scc = false, smdc = false;
        String prop = System.getProperty(LitelinksSystemPropNames.CLIENT_THREADCONTEXTS);
        if (prop != null) {
            for (String v : prop.split(",")) {
                if ("custom".equals(v)) {
                    scc = true;
                } else if ("log4j_mdc".equals(v) || "log_mdc".equals(v)) {
                    smdc = true;
                }
            }
        }
        return new boolean[] { scc, smdc };
    }

    // non-static to facilitate unit tests
    private final boolean usePrivateEndpoints;
    private final String privateDomainId;

    {
        String up = System.getProperty(LitelinksSystemPropNames.USE_PRIVATE_ENDPOINTS);
        if (up == null) {
            up = System.getenv(LitelinksEnvVariableNames.USE_PRIVATE_ENDPOINTS);
        }
        if (up != null) {
            usePrivateEndpoints = Boolean.parseBoolean(up);
        } else {
            String pe = System.getProperty(LitelinksSystemPropNames.PRIVATE_ENDPOINT);
            if (pe == null || pe.trim().isEmpty()) {
                pe = System.getenv(LitelinksEnvVariableNames.PRIVATE_ENDPOINT);
            }
            usePrivateEndpoints = pe != null && !pe.trim().isEmpty();
        }
        logger.info("Litelinks clients will use private endpoints when available: " + usePrivateEndpoints);

        String did = System.getProperty(LitelinksSystemPropNames.PRIVATE_DOMAIN_ID);
        if (did == null) {
            did = System.getenv(LitelinksEnvVariableNames.PRIVATE_DOMAIN_ID);
        }
        privateDomainId = did;
        if (did != null) {
            logger.info("Litelinks private network domain id: " + did);
        } else if (usePrivateEndpoints) {
            logger.warn("Private endpoints configured, but no network domain set");
        }
    }

    public boolean usePrivateEndpoints() {
        return usePrivateEndpoints;
    }

    public String privateDomainId() {
        return privateDomainId;
    }

    protected static final Pattern HOSTNAME_PATT = Pattern.compile("[\\w.-]+"); // only rudimentary validation
    protected static final Pattern PRIV_ENDPOINT_PATT = Pattern.compile("([^\\s:;]+):(\\d+)(?:;([^\\s;]+))?");

    public class ThriftInstanceConfig extends ServiceInstanceConfig<C> {
        private final String host;
        private final int port;

        private final boolean framed;
        private final TProtocolFactory protoFactory;

        private final String sslProtocol;
        private final SslContext sslContext;

        private final boolean extraInfoSupported;

        public ThriftInstanceConfig(String host, int port, String version,
                long registrationTime, Map<Object, Object> connConfig,
                Map<String, String> metadata, Map<String, MethodInfo> methodInfos) throws Exception {
            super(version, registrationTime, metadata, methodInfos);
            // default is framed=false
            this.framed = "true".equals(connConfig.get(ThriftConnProp.TR_FRAMED));
            this.protoFactory = getServiceProtocolFactory(connConfig);

            this.extraInfoSupported = "true".equals(connConfig.get(ThriftConnProp.TR_EXTRA_INFO));

            final boolean ssl = "true".equals(connConfig.get(ThriftConnProp.TR_SSL));
            if (ssl) {
                String protocol = (String) connConfig.get(ThriftConnProp.TR_SSL_PROTOCOL);
                this.sslProtocol = protocol != null ? protocol : SSLParams.getDefault().protocol;
                this.sslContext = SSLHelper.getSslContext(sslProtocol, false, false);
            }
            else {
                this.sslProtocol = null;
                this.sslContext = null;
            }

            if (usePrivateEndpoints()) {
                String privateEndpoint = (String) connConfig.get(ThriftConnProp.PRIVATE_ENDPOINT);
                if (privateEndpoint != null) {
                    Matcher m = PRIV_ENDPOINT_PATT.matcher(privateEndpoint);
                    if (!m.matches()) {
                        logger.warn("discovered invalid internal address "
                                    + "for instance " + host + ":" + port + " : " + privateEndpoint);
                    } else {
                        // if either our domain or the target domain isn't set,
                        // assume we are in the *same* domain (for backward compatibility)
                        String ourDomain = privateDomainId();
                        String domain = ourDomain == null ? null : m.group(3);
                        if (domain == null || Objects.equals(domain, ourDomain)) {
                            host = m.group(1);
                            port = Integer.parseInt(m.group(2));
                        }
                    }
                }
            }

            this.host = host;
            this.port = port;
        }

        public C createClient(long timeoutMillis) throws TTransportException {
            final TTransport trans = transFactory.openNewTransport(host, port,
                    timeoutMillis, framed, sslContext);
            if (logger.isDebugEnabled()) {
                logger.debug("Successfully opened new "
                             + (sslContext != null ? "TLS " : "") + "transport to " + host + ":" + port);
            }
            TProtocol in = protoFactory.getProtocol(trans);
            TProtocol out = extraInfoSupported ?
                    new InterceptTOutProto(in, sendMDC) : in;
            return clientFactory.getClient(in, out);
        }

        @Override
        public String getHost() {
            return host;
        }

        @Override
        public int getPort() {
            return port;
        }

        @Override
        public int hashCode() {
            return Objects.hash(getVersion(), framed, extraInfoSupported, host, port,
                    protoFactory != null ? protoFactory.getClass() : null,
                    sslContext == null, sslProtocol);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null) return false;
            if (getClass() != obj.getClass()) return false;
            @SuppressWarnings("unchecked")
            ThriftInstanceConfig other = (ThriftInstanceConfig) obj;
            if (framed != other.framed)	return false;
            if (extraInfoSupported != other.extraInfoSupported) return false;
            if (!Objects.equals(host, other.host)) return false;
            if (port != other.port)	return false;
            if ((sslContext == null) != (other.sslContext == null)) return false;
            if (!Objects.equals(sslProtocol, other.sslProtocol)) return false;
            Class<?> pfc = protoFactory != null ? protoFactory.getClass() : null;
            Class<?> opfc = protoFactory != null ? protoFactory.getClass() : null;
            if (!Objects.equals(pfc, opfc)) return false;
            if (Objects.equals(getVersion(), other.getVersion())) return false;
            return true;
        }
    }

    static TProtocolFactory getServiceProtocolFactory(Map<Object, Object> props) {
        TProtocolFactory tpf = DEFAULT_TPROTOFAC;
        String tp = (String) props.get(ThriftConnProp.TR_PROTO_FACTORY);
        if (tp != null) {
            try {
                Class<?> tpc = Class.forName(tp);
                tpf = (TProtocolFactory) tpc.newInstance();
            } catch (Exception e) {
                logger.warn("Unrecognized TProtocolFactory: " + tp);
                // (fallback to default)
            }
        }
        return getOptimizedTProtoFactory(tpf);
    }

    // used internally only, public for unit tests
    public String getServiceName() {
        return serviceName;
    }

    static class ServiceKey {
        final String name;
        final String mplexerName;
        final ServiceRegistryClient svcRegistry;
        final Class<?> serviceClass;

        public ServiceKey(String name, String mplexerName, Class<?> serviceClass, ServiceRegistryClient sdConfig) {
            this.name = name;
            this.mplexerName = mplexerName;
            svcRegistry = sdConfig;
            this.serviceClass = serviceClass;
        }
        @Override
        public int hashCode() {
            return Objects.hash(name, mplexerName, serviceClass, svcRegistry);
        }
        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null) return false;
            if (getClass() != obj.getClass()) return false;
            final ServiceKey other = (ServiceKey) obj;
            return Objects.equals(name, other.name) &&
                   Objects.equals(mplexerName, other.mplexerName) &&
                   Objects.equals(svcRegistry, other.svcRegistry) &&
                   Objects.equals(serviceClass, other.serviceClass);
        }
    }

    public static class TGenericServiceClient extends TServiceClient {
        public TGenericServiceClient(TProtocol iprot, TProtocol oprot) {
            super(iprot, oprot);
        }
        public static class Factory implements TServiceClientFactory<TGenericServiceClient> {
            @Override
            public TGenericServiceClient getClient(TProtocol prot) {
                return new TGenericServiceClient(prot, prot);
            }
            @Override
            public TGenericServiceClient getClient(TProtocol iprot, TProtocol oprot) {
                return new TGenericServiceClient(iprot, oprot);
            }
        }
    }

    static class TMultiplexClientFactory<T extends TServiceClient> implements TServiceClientFactory<T> {
        private final TServiceClientFactory<T> delegate;
        private final String serviceName;

        public TMultiplexClientFactory(TServiceClientFactory<T> delegate, String serviceName) {
            this.delegate = delegate;
            this.serviceName = serviceName;
        }
        @Override
        public T getClient(TProtocol tp) {
            return delegate.getClient(new TMultiplexedProtocol(tp, serviceName));
        }
        @Override
        public T getClient(TProtocol intp, TProtocol outtp) {
            return delegate.getClient(intp, new TMultiplexedProtocol(outtp, serviceName));
        }
    }
}
