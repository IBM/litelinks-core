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

import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.Service;
import com.ibm.watson.litelinks.LitelinksEnvVariableNames;
import com.ibm.watson.litelinks.LitelinksSystemPropNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class WatchedService extends AbstractService implements ListeningService, NamedService {

    private static final Logger logger = LoggerFactory.getLogger(WatchedService.class);

    protected static final Pattern HOSTNAME_PATT = Pattern.compile("[\\w.-]+"); // only rudimentary validation
    protected static final Pattern PRIV_ENDPOINT_PATT = Pattern.compile("([^\\s:;]+)(?::(\\d+))?(?:;([^\\s;]+))?");

    private final SettableServiceDeploymentInfo deploymentInfo;
    private final Service monitoredService;

    private final int healthProbePort;

    protected final ScheduledExecutorService eventThreads;

    private Future<?> initFuture;

    private ProbeHttpServer healthProbe;

    private DeployedService deployedService() {
        return monitoredService instanceof DeployedService
                ? (DeployedService) monitoredService : DeployedService.NO_OP;
    }

    public WatchedService(Service monitoredService, String serviceName,
            String serviceVersion, String instanceId, int port, int probePort) {
        if (serviceName != null && serviceName.trim().isEmpty()) {
            throw new IllegalArgumentException("service name can't be empty");
        }
        this.monitoredService = monitoredService;
        healthProbePort = probePort;
        eventThreads = LitelinksService.getServiceEventThreads();
        deploymentInfo = new SettableServiceDeploymentInfo();
        deploymentInfo.setServiceName(serviceName);
        deploymentInfo.setServiceVersion(serviceVersion);
        deploymentInfo.setPublicPort(port);

        /* NOTE in future the default instance id might incorporate the
         * port, but this would require change in initialization order
         * since currently the port may not have been assigned when the
         * service impl and req listeners are initialized.
         */
        if (instanceId == null) {
            instanceId = UUID.randomUUID().toString();
        }
        deploymentInfo.setInstanceId(instanceId);

        String privateAddress = determinePrivateEndpoint();
        if (privateAddress != null) {
            int privatePort;
            Matcher m = PRIV_ENDPOINT_PATT.matcher(privateAddress);
            if (!m.matches()) {
                throw new IllegalArgumentException("invalid private address provided: " + privateAddress);
            }
            String portStr = m.group(2);
            if (portStr != null) {
                privatePort = Integer.parseInt(portStr);
            } else {
                // private port defaults to public port if none specified
                privatePort = port;
            }
            String privateDomainId = m.group(3);
            if (privateDomainId == null) {
                privateDomainId = determinePrivateDomainId();
            }
            deploymentInfo.setPrivateAddress(m.group(1));
            deploymentInfo.setPrivatePort(privatePort);
            deploymentInfo.setPrivateDomain(privateDomainId);
        }
        // else private address not configured and won't be published
    }

    @Override
    protected void doStart() {
        try {
            deploymentInfo.setPublicAddress(determineHostString());
            logger.info("Starting service-watching wrapper; hostname=" + deploymentInfo.getPublicAddress());
            deployedService().setDeploymentInfo(deploymentInfo);
            // monitor the service, we use the callback for registering and deregistering
            monitoredService.addListener(listener, eventThreads);

            if (healthProbePort > 0) {
                try {
                    healthProbe = new ProbeHttpServer(healthProbePort,
                            deployedService()::isReady,
                            deployedService()::isLive, false);
                    addListener(new Listener() {
                        @Override
                        public void failed(State from, Throwable failure) {
                            healthProbe.close();
                        }
                        @Override
                        public void terminated(State from) {
                            healthProbe.close();
                        }
                    }, Runnable::run);
                } catch (Exception e) {
                    String message = "Error starting http health probe server on port " + healthProbePort;
                    logger.error(message, e);
                    notifyFailed(new Exception(message, e));
                }
            }

            // initialize service registration mechanism (async)
            initFuture = eventThreads.submit(() -> {
                initialize();
                return null;
            });

            // start the service, we're going to wait for the monitored service to start
            // and when it does we will register (see running() method below)
            monitoredService.startAsync();
        } catch (UnknownHostException e) {
            notifyFailed(e);
        }
    }

    private static final long DEREG_PAUSE_MILLIS = 2000; //TODO TBD; maybe impl-specific time

    @Override
    protected void doStop() {
        if (!isRegistered()) {
            Future<?> inFut = initFuture;
            if (inFut != null) {
                inFut.cancel(false);
            }
            monitoredService.stopAsync();
        } else {
            eventThreads.execute(() -> {
                try {
                    try {
                        deployedService().preShutdown();
                    } catch (Throwable t) {
                        logger.error("Error in impl pre-shutdown tasks", t);
                        if (t instanceof Error) {
                            throw t; // else proceed with shutdown
                        }
                    }
                    boolean wasRegistered = doDeregister();
                    if (monitoredServiceIsStopped()) {
                        notifyStopped();
                    } else {
                        // now take down the service
                        if (!wasRegistered) {
                            monitoredService.stopAsync();
                        } else {
                            // give time for deregistration to propagate to clients
                            logger.info("waiting " + DEREG_PAUSE_MILLIS
                                        + " milliseconds before stopping service");
                            eventThreads.schedule(monitoredService::stopAsync,
                                    DEREG_PAUSE_MILLIS, TimeUnit.MILLISECONDS);
                        }
                    }
                } catch (Throwable t) {
                    notifyFailed(t);
                }
            });
        }
    }

    protected ConfiguredService getConfiguredService() {
        return monitoredService instanceof ConfiguredService?
                (ConfiguredService) monitoredService : null;
    }

    /**
     * Should be used to do any initialization of the service registration mechanism
     * possible *prior* to actual registration of the service. Is called in parallel
     * with the service implementation's initializtion.
     */
    protected void initialize() throws Exception {}

    /**
     * Must be threadsafe (idempotent w.r.t. multiple invocations)
     *
     * @return true if service was registered prior to invocation, false otherwise
     */
    protected abstract boolean deregister(); //TODO exceptions TBD

    private boolean doDeregister() {
        if (healthProbe != null) {
            healthProbe.setReady(false);
        }
        return deregister();
    }

    /**
     * Should notify successful registration using {@link #notifyStarted()}. In case of
     * failures should throw exception or call {@link #failedWhileStarting(Throwable)}.
     * Guaranteed to be called only after {@link #initialize()} has returned.
     *
     * @throws Exception
     */
    protected abstract void registerAsync() throws Exception; //TODO exceptions tbd

    /**
     * @return true if registered <b>OR</b> registration in progress, false otherwise
     */
    protected abstract boolean isRegistered();

    private boolean monitoredServiceIsStopped() {
        return State.STOPPING.compareTo(monitoredService.state()) < 0;
    }

    private Throwable pendingFailure;

    protected final Listener listener = new Listener() {
        @Override
        public void failed(State from, Throwable failure) {
            // if the dependent service fails then we fail
            try {
                initFuture.cancel(false);
                doDeregister();
            } finally {
                notifyFailed(failure);
            }
        }

        @Override
        public void terminated(State from) {
            logger.info("watched service terminated, previous state=" + from);
            if (pendingFailure != null) {
                notifyFailed(pendingFailure);
            } else {
                // should already be deregistered, but this is for cases where
                // the monitored service stopped unexpectedly (not initiated by us)
                initFuture.cancel(false);
                doDeregister();
                notifyStopped();
            }
        }

        @Override
        public void running() {
            logger.debug("watched service has started");
            try {
                if (!initFuture.isDone()) {
                    logger.info("Waiting for service registration initialization...");
                }
                try {
                    initFuture.get(LitelinksService.getMaxShutdownTimeoutMs(), TimeUnit.MILLISECONDS);
                } catch (ExecutionException e) {
                    throw new Exception("Service registration init failed", e.getCause());
                } catch (TimeoutException | InterruptedException e) {
                    throw new Exception("Service registration init timed-out or interrupted", e);
                }
                String name = getServiceName(), version = getServiceVersion(), iid = getInstanceId();
                validateServiceParams(name, version, iid);
                logger.info("About to register service '" + name + "'"
                            + (version != null? " with version '" + version + "'" : "")
                            + (iid != null? ", instanceId = '" + iid + "'" : ""));
                if (healthProbe != null) {
                    healthProbe.setReady(true);
                }
                registerAsync();
            } catch (Throwable t) {
                failedWhileStarting(t);
            }
        }
    };

    protected void failedWhileStarting(final Throwable t) {
        // dereg and stop monitored service
        try {
            doDeregister();
            pendingFailure = t; // failure notification will happen in listener terminated() or failed()
            monitoredService.stopAsync();
        } catch (Throwable t2) {
            logger.warn("Exception stopping service after failure", t2);
            notifyFailed(t);
        }
    }


    @Override
    public SocketAddress getListeningAddress() {
        return deploymentInfo.getListeningAddress();
    }

    // the port to register
    public int getPublicPort() {
        return deploymentInfo.getPublicPort();
    }

    @Override
    public String getServiceName() {
        return deploymentInfo.getServiceName();
    }

    @Override
    public String getServiceVersion() {
        return deploymentInfo.getServiceVersion();
    }

    public String getInstanceId() {
        return deploymentInfo.getInstanceId();
    }

    public String getHost() throws UnknownHostException {
        String hn = deploymentInfo.getPublicAddress();
        return hn != null? hn : determineHostString();
    }

    /**
     * @return host:port or null if not configured
     */
    public String getPrivateEndpointString() {
        String privateHost = deploymentInfo.getPrivateAddress();
        if (privateHost == null) {
            return null;
        }
        int privatePort = deploymentInfo.getPrivatePort();
        String privateDomain = deploymentInfo.getPrivateDomain();
        return privatePort == -1? null :
                privateHost + ":" + privatePort +
                (privateDomain != null? ";" + privateDomain : "");
    }

    private static void validateServiceParams(String name, String version, String instanceId)
            throws Exception {
        if (name != null && name.indexOf('\n') >= 0) {
            throw new Exception("Service name can't contain line break");
        }
        if (instanceId != null && instanceId.indexOf('\n') >= 0) {
            throw new Exception("Service instance id can't contain line break");
        }
        if (version != null && version.indexOf('\n') >= 0) {
            throw new Exception("Service version string can't contain line break");
        }
    }

    protected static String determinePrivateEndpoint() {
        String pe = System.getProperty(LitelinksSystemPropNames.PRIVATE_ENDPOINT);
        return pe != null? pe : System.getenv(LitelinksEnvVariableNames.PRIVATE_ENDPOINT);
    }

    protected static String determinePrivateDomainId() {
        String pd = System.getProperty(LitelinksSystemPropNames.PRIVATE_DOMAIN_ID);
        return pd != null? pd : System.getenv(LitelinksEnvVariableNames.PRIVATE_DOMAIN_ID);
    }

    protected static String determineHostString() throws UnknownHostException {
        String hostname = System.getenv(LitelinksEnvVariableNames.ADDRESS);
        if (hostname == null) {
            return findHost();
        }
        if (!HOSTNAME_PATT.matcher(hostname).matches()) {
            throw new UnknownHostException("Invalid host address provided via "
                                           + LitelinksEnvVariableNames.ADDRESS + " env var: \"" + hostname + "\"");
        }
        return hostname;
    }

    // defaults to IP address
    protected static String findHost() throws UnknownHostException {
        InetAddress address = InetAddress.getLocalHost();
        boolean useHostname = "localhostname".equals(
                System.getProperty(LitelinksSystemPropNames.DEFAULT_PUBLISH_ADDR));
        return useHostname? address.getCanonicalHostName() : address.getHostAddress();
    }

    @Override
    public String toString() {
        return getServiceName();
    }
}
