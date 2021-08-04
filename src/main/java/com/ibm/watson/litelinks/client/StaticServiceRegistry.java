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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.ibm.watson.litelinks.ThriftConnProp;
import org.apache.thrift.protocol.TProtocolFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Static service registry client implementation - specify an
 * explicit server to connect to instead of discovering the
 * service dynamically.
 */
public class StaticServiceRegistry implements ServiceRegistryClient {

    private final String hostname;
    private final int port;

    private final Class<? extends TProtocolFactory> protoFactory;
    private final Boolean framed;
    private final Boolean ssl;
    private final Boolean extraInfo;

    public StaticServiceRegistry(String hostname, int port) {
        this(hostname, port, null, null, null, null);
    }

    public StaticServiceRegistry(String hostname, int port,
            Class<? extends TProtocolFactory> protoFactory,
            Boolean framed, Boolean ssl, Boolean extraInfo) {
        this.hostname = hostname;
        this.port = port;
        this.protoFactory = protoFactory;
        this.framed = framed;
        this.ssl = ssl;
        this.extraInfo = extraInfo;
    }

    /**
     * @param target of the form "host:port?ssl=X&amp;framed=Y&amp;extraInfo=Z&amp;protoFac=PF"
     *               where the "query string" part and individual params within it are optional
     */
    public StaticServiceRegistry(String target) {
        // crude parsing
        String[] parts = target.split("\\?"), server = parts[0].split(":");
        if (server.length != 2) {
            throw new IllegalArgumentException(target);
        }
        hostname = server[0];
        port = Integer.parseInt(server[1]);
        Boolean ssl = null, framed = null, extraInfo = null;
        String protoFac = null;
        if (parts.length > 1) {
            for (String param : parts[1].split("&")) {
                String[] kv = param.split("=");
                if (kv.length != 2) {
                    throw new IllegalArgumentException(target);
                }
                switch (kv[0]) {
                case "ssl":
                    ssl = Boolean.parseBoolean(kv[1]);
                    break;
                case "framed":
                    framed = Boolean.parseBoolean(kv[1]);
                    break;
                case "extraInfo":
                    extraInfo = Boolean.parseBoolean(kv[1]);
                    break;
                case "protoFac":
                    protoFac = kv[1];
                    break;
                default:
                    throw new IllegalArgumentException(target);
                }
            }
        }
        this.ssl = ssl;
        this.framed = framed;
        this.extraInfo = extraInfo;
        try {
            protoFactory = protoFac == null? null
                    : Class.forName(protoFac).asSubclass(TProtocolFactory.class);
        } catch (ClassNotFoundException cnfe) {
            throw new IllegalArgumentException("protofac class not found: " + protoFac, cnfe);
        }
    }

    @Override
    public ServiceWatcher newServiceWatcher(final String serviceName) {
        return new ServiceWatcher(serviceName) {
            @Override
            protected ServiceRegistryClient getRegistryClient() {
                return StaticServiceRegistry.this;
            }

            @Override
            public void start(long timeoutMillis) throws Exception {
                Map<Object, Object> props = new HashMap<>();
                if (protoFactory != null) {
                    props.put(ThriftConnProp.TR_PROTO_FACTORY,
                            protoFactory.getName());
                }
                if (ssl != null) {
                    props.put(ThriftConnProp.TR_SSL, ssl.toString());
                }
                if (framed != null) {
                    props.put(ThriftConnProp.TR_FRAMED, framed.toString());
                }
                if (extraInfo != null) {
                    props.put(ThriftConnProp.TR_EXTRA_INFO, extraInfo.toString());
                }
                String id = hostname + ":" + port + "/" + serviceName;
                getListener().serverAdded(hostname, port, System.currentTimeMillis(),
                        null, id, id, props);
            }

            @Override
            protected ListenableFuture<Void> startAsync() {
                try {
                    start(0L);
                    return Futures.immediateFuture(null);
                } catch (Exception e) {
                    return Futures.immediateFailedFuture(e);
                }
            }

            @Override
            protected boolean isAvailable() {
                return true;
            }

            @Override
            public boolean confirmUnavailable() {
                return false;
            }

            @Override
            public void close() {}
        };
    }

    @Override
    public int hashCode() {
        return Objects.hash(hostname, port, protoFactory, framed, ssl);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        StaticServiceRegistry other = (StaticServiceRegistry) obj;
        if (!Objects.equals(hostname, other.hostname)) return false;
        if (port != other.port)	return false;
        if (!Objects.equals(protoFactory, other.protoFactory)) return false;
        if (!Objects.equals(framed, other.framed)) return false;
        if (!Objects.equals(ssl, other.ssl)) return false;
        return true;
    }
}
