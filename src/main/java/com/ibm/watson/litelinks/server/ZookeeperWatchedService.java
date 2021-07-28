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

import com.google.common.primitives.Bytes;
import com.google.common.util.concurrent.Service;
import com.ibm.watson.litelinks.ThriftConnProp;
import com.ibm.watson.litelinks.server.ConfiguredService.ConfigMismatchException;
import com.ibm.watson.zk.ZookeeperClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ZookeeperWatchedService extends WatchedService {

    private static final Logger logger = LoggerFactory.getLogger(ZookeeperWatchedService.class);

    public static final String DEFAULT_ZK_SERVICES_PARENT = "/services";

    public static final String SERVICE_VERSION = "SERVICE_VERSION";
    public static final String INSTANCE_ID = "INSTANCE_ID";

    /**
     * @see org.apache.curator.framework.api.CreateBuilder#withProtection()
     */
    protected static final boolean USE_PROTECTION = true;

    private final String zookeeperConnString;
//	private static final int HEARTBEAT_LENGTH = 5;

    private CuratorFramework curator;
    private volatile PersistentNode pen;

    public static String getServicePath(String serviceName) {
        return ZKPaths.makePath(DEFAULT_ZK_SERVICES_PARENT, serviceName);
    }

    public ZookeeperWatchedService(Service monitoredService, String zookeeperConnString,
            String serviceName, String serviceVersion, String instanceId, int port, int probePort) {
        super(monitoredService, serviceName, serviceVersion, instanceId, port, probePort);
        this.zookeeperConnString = zookeeperConnString;
    }

    public <S extends Service & ListeningService> ZookeeperWatchedService(S monitoredService,
            String zookeeperConnString, String serviceName) {
        this(monitoredService, zookeeperConnString, serviceName, null, null, 0, 0); //TODO pass in
    }

    public <S extends Service & ListeningService> ZookeeperWatchedService(S monitoredService,
            String zookeeperConnString) {
        this(monitoredService, zookeeperConnString, null, null, null, 0, 0);
    }

    @Override
    protected boolean deregister() {
        PersistentNode pen = this.pen;
        if (pen != null) {
            synchronized (pen) {
                if (this.pen != null) {
                    try {
                        String path = pen.getActualPath();
                        // takedown of zookeeper ephemeral node
                        logger.info("about to deregister service");
                        pen.close();
                        this.pen = null;
                        logger.info("service ephemeral znode closed");
                        int lastseg = path != null? path.lastIndexOf('/') : -1;
                        if (lastseg >= 0) {
                            try {
                                // try to clean up service node, will fail if other instances
                                // are registered
                                curator.delete().forPath(path.substring(0, lastseg));
                            } catch (KeeperException.NotEmptyException |
                                    KeeperException.NoNodeException e) {
                                // no problem - still in use or already gone
                            }
                        }
                        return true;
                    } catch (Exception e) {
                        logger.warn("Exception closing ephemeral znode", e);
                    }
                }
            }
        }
        return false;
    }

    /*
     * TODO: For some reason, moving the initial ZK conn establishment to the
     * initialize phase causes LitelinksLauncherTests unit tests to hang when
     * running as part of maven/surefire build in Jenkins. The problem appears
     * to be with separate service procs which are spawned, in particular some
     * number of them remain running when they shouldn't.
     *
     * So, keeping initial ZK conn in the registerAsync() method for now until
     * this is understood/rectified.
     */
//	@Override
//	protected void initialize() throws Exception {
//		curator = ZookeeperClient.getCurator(zookeeperConnString, true);
//		if (curator == null) return; //TODO maybe throw exception
//		logger.info("connected to zookeeper using connString: "+curator.getZookeeperClient().getCurrentConnectionString());
//	}

    private static final int ZNODE_CREATE_TIMEOUT_SECS = 10; //TODO

    private static final byte[] DELIM_BYTES = ":\n".getBytes(StandardCharsets.ISO_8859_1), EMPTY = new byte[0];

    @Override
    protected void registerAsync() throws Exception {
        curator = ZookeeperClient.getCurator(zookeeperConnString, true);

        //if we don't have a zookeeper then do nothing
        //TODO maybe disallow this
        if (curator == null) {
            logger.info("zookeeper not configured; skipping service registration");
            notifyStarted();
            return;
        }
        logger.info("connected to zookeeper using connString: " +
                    curator.getZookeeperClient().getCurrentConnectionString());

        final String serviceName = getServiceName();
        if (serviceName == null) {
            throw new IllegalStateException("Could not determine name for service");
        }
        if (serviceName.indexOf('/') >= 0) {
            throw new IllegalArgumentException("service name must not contain '/'");
        }

        final String servicePath = getServicePath(serviceName);

        final String version = getServiceVersion();

        final String instanceId = getInstanceId();

        final String serviceLocation = getHost() + ":" + getPublicPort();

        final String privateEndpoint = getPrivateEndpointString();

        logger.info("registering service with zookeeper: " + servicePath + ", endpoint " + serviceLocation
                    + ", private endpoint " + (privateEndpoint != null? privateEndpoint : "none")
                    + ", version " + (version != null? version : "not specified"));

        // now setup service znode and set config / verify config consistency
        Stat stat = null;
        final ConfiguredService cservice = getConfiguredService();
        final byte[] config = cservice != null? serializeConfig(cservice.getConfig()) : EMPTY;
        if (config != EMPTY) {
            /*
             * The following logic is for backwards compatibility with older versions of litelinks (< v1.0.0).
             * Old clients look for service config only in the parent "service" znode, not the ephemeral
             * service instance znode, so we must also store it there. The aim is for the first registered
             * instance to write the shared config and for subsequent ones to validate that their config
             * is compatible with it. Closing all race conditions here would mean we wouldn't be able to use
             * Curator's PersistentNode as-is (it creates missing parent nodes with empty data, and doesn't
             * use transactions), and per-instance config takes precedence with newer clients anyhow.
             */
            createloop: while (true) {
                try {
                    stat = null;
                    curator.create().creatingParentContainersIfNeeded().forPath(servicePath, config);
                    break;
                } catch (KeeperException.NodeExistsException nee) {
                    byte[] data;
                    stat = new Stat();
                    while (true) {
                        try {
                            data = curator.getData().storingStatIn(stat).forPath(servicePath);
                        } catch (KeeperException.NoNodeException nne) {
                            continue createloop;// ok, continue loop
                        }
                        // if service znode has no children and was created > 20sec ago, assume it's "old" so
                        // delete/recreate it with our config
                        if (stat.getNumChildren() == 0 && System.currentTimeMillis() - stat.getMtime() > 20_000L) {
                            try {
                                curator.delete().withVersion(stat.getVersion()).forPath(servicePath);
                                continue createloop;
                            } catch (KeeperException.NoNodeException nne) {
                                continue createloop;
                            } catch (KeeperException.BadVersionException bve) {
                                continue;
                            } catch (KeeperException.NotEmptyException ntee) {}
                        }
                        cservice.verifyConfig(deserializeConfig(data));
                        break createloop;
                    }
                }
            }
        }

        byte[] headerData = serviceLocation.getBytes(StandardCharsets.ISO_8859_1);
        byte[] iidData = instanceId == null? EMPTY  // instanceId should never be null
                : (INSTANCE_ID + "=" + instanceId + "\n").getBytes(StandardCharsets.ISO_8859_1);
        byte[] versData = version == null? EMPTY
                : (SERVICE_VERSION + "=" + version + "\n").getBytes(StandardCharsets.ISO_8859_1);
        byte[] privEndpointData = privateEndpoint == null? EMPTY
                : (ThriftConnProp.PRIVATE_ENDPOINT + "=" + privateEndpoint + "\n").getBytes(StandardCharsets.ISO_8859_1);
        byte[] instanceData = Bytes.concat(headerData, DELIM_BYTES, iidData, versData, privEndpointData, config);

        logger.info("creating service ephemeral znode...");
        // with "protection" on, the node names will end up looking like: "_c_6aa0c8be-ff7b-4171-9cff-a2dfbde6e0d4-i-0000000000"
        pen = new PersistentNode(curator, CreateMode.EPHEMERAL_SEQUENTIAL, USE_PROTECTION,
                servicePath + (USE_PROTECTION? "/i-" : "/instance-"), instanceData);
        pen.start();

        final Stat fstat = stat;
        //we're going to wait 10 seconds for initial connection. If it doesn't connect, we're going to bring down the service
        eventThreads.execute(() -> {
            try {
                boolean started = pen.waitForInitialCreate(ZNODE_CREATE_TIMEOUT_SECS, TimeUnit.SECONDS);
                if (!started) {
                    throw new Exception("waited " + ZNODE_CREATE_TIMEOUT_SECS
                            + " seconds and failed to create service instance znode");
                }
                if (config != EMPTY) {
                    // ensure that config wasn't changed.
                    Stat newStat;
                    if (fstat != null) {
                        newStat = curator.checkExists().forPath(servicePath);
                        if (newStat == null) {
                            newStat = new Stat();
                        } else if (newStat.getMzxid() == fstat.getMzxid()) {
                            newStat = null; // good
                        }
                    } else {
                        newStat = new Stat();
                    }
                    if (newStat != null) {
                        while (true) {
                            byte[] newdata = curator.getData().storingStatIn(newStat).forPath(servicePath);
                            if (newdata != null && newdata.length > 0) {
                                cservice.verifyConfig(deserializeConfig(newdata));
                                break; // good
                            } else {
                                try {
                                    newStat = curator.setData().withVersion(newStat.getVersion())
                                            .forPath(servicePath, config);
                                    break; // good
                                } catch (KeeperException.BadVersionException bve) {} // continue get/set loop
                            }
                        }
                    }
                }
                logger.info("service ephemeral znode created: " + pen.getActualPath());
                notifyStarted();
            } catch (Throwable t) {
                failedWhileStarting(t); // this will call deregister()
            }
        });
    }

    @Override
    protected boolean isRegistered() {
        return pen != null;
    }

    private static byte[] serializeConfig(Map<String, String> config) throws Exception {
//		if(config == null) return null;
//		Properties props = new Properties();
//		props.putAll(config);
//		ByteArrayOutputStream baos = new ByteArrayOutputStream();
//		props.store(baos, "");
//		return baos.toByteArray();
        final StringBuilder sb = new StringBuilder();
        for (Entry<String, String> ent : config.entrySet()) {
            if (sb.length() > 0) {
                sb.append('\n');
            }
            sb.append(escape(ent.getKey(), true)).append('=')
                    .append(escape(ent.getValue(), false));
        }
        return sb.toString().getBytes(StandardCharsets.ISO_8859_1);
    }

    private static Map<String, String> deserializeConfig(byte[] configData) throws ConfigMismatchException {
        final Properties other = new Properties();
        try {
            other.load(new ByteArrayInputStream(configData));
            return downcastMap(other);
        } catch (IOException e) {
            throw new ConfigMismatchException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T, U extends T> Map<U, U> downcastMap(Map<T, T> map) {
        return (Map<U, U>) map;
    }

    /*
     * This mimics escaping logic in java.util.Properties
     */
    private static CharSequence escape(String str, boolean escSpace) {
        int len = str.length(), blen = len * 2;
        if (blen < 0) {
            blen = Integer.MAX_VALUE;
        }
        StringBuilder out = new StringBuilder(blen);
        for (int i = 0; i < len; i++) {
            final char c = str.charAt(i);
            if (c > 61 && c < 127) {
                if (c == '\\') {
                    out.append("\\\\");
                } else {
                    out.append(c);
                }
            } else {
                switch (c) {
                case ' ':
                    if (i == 0 || escSpace) {
                        out.append('\\');
                    }
                    out.append(' ');
                    break;
                case '\t':out.append("\\t"); break;
                case '\n':out.append("\\n"); break;
                case '\r':out.append("\\r"); break;
                case '\f':out.append("\\f"); break;
                case '=': case ':': case '#': case '!':
                    out.append('\\').append(c);
                    break;
                default:
                    if (c < 0x0020 || c > 0x007e) {
                        out.append("\\u").append(hex(c >> 12));
                        out.append(hex(c >> 8)).append(hex(c >> 4));
                        out.append(hex(c));
                    } else {
                        out.append(c);
                    }
                }
            }
        }
        return out;
    }

    private static final char[] hexChars = "0123456789ABCDEF".toCharArray();

    private static char hex(int nib) {
        return hexChars[nib & 0xF];
    }

//	protected String findHostName() throws UnknownHostException {
//		zkConnString = ZookeeperClient.resolveConnString(zookeeperConnString);
//		if(zkConnString != null) {
//			List<InetSocketAddress> sads = new ConnectStringParser(zookeeperConnString).getServerAddresses();
//			if(!sads.isEmpty()) {
//				InetAddress zka = sads.get(0).getAddress();
//
//			}
//		}
//	}
}
