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

import com.google.common.io.Files;
import com.ibm.watson.litelinks.InvalidThriftClassException;
import com.ibm.watson.litelinks.client.LitelinksServiceClient;
import com.ibm.watson.litelinks.client.ThriftClientBuilder;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.TProcessor;

import java.io.File;
import java.lang.reflect.Constructor;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static com.ibm.watson.litelinks.server.AdapterThriftService.getTProcConstructorFromIface;

/**
 * Service which acts as a proxy for multiple other services, multiplexing them
 * over a single port (per instance of the proxy).
 * <p>
 * Requires TARGET_ZOOKEEPER env var or jvm arg,
 * proxiedServiceList jvm arg with path to config file.
 */
public class MultiplexingProxyService extends ThriftService {

    public static final String TARGET_ZK_EV = "TARGET_ZOOKEEPER";
    public static final String SVC_LIST_FILE_ARG = "proxiedServiceList";

    List<LitelinksServiceClient> clients;

    @Override
    protected TProcessor initialize() throws Exception {
        String ZK = System.getProperty(TARGET_ZK_EV);
        if(ZK==null) ZK = System.getenv(TARGET_ZK_EV);
        if(ZK==null) throw new Exception(TARGET_ZK_EV+" env var must be provided");

        String configFile = System.getProperty(SVC_LIST_FILE_ARG);
        if (configFile == null) {
            throw new Exception(SVC_LIST_FILE_ARG + " JVM arg must be provided");
        }
        // below can throw FNF
        List<String> lines = Files.readLines(new File(configFile), StandardCharsets.UTF_8);
        clients = new ArrayList<>(lines.size());
        final TMultiplexedProcessor processor = new TMultiplexedProcessor();
        for (String line : lines) {
            int ci = line.indexOf('#');
            if(ci == 0) continue;
            if(ci > 0) line = line.substring(0, ci);
            if((line = line.trim()).isEmpty()) continue;
            final String[] toks = line.split("\\s+");
            if (toks.length < 2) {
                throw new Exception("Invalid entry in config file: " + line);
            }
            final String svcName = toks[0];
            final Class<?> svcClass = Class.forName(toks[1]);
            final Class<?> iface = AdapterThriftService.getIfaceFromSvcClass(svcClass);
            if (iface == null) {
                throw new InvalidThriftClassException(svcClass.getName());
            }
            // build client for proxied service
            final ThriftClientBuilder<?> builder = ThriftClientBuilder.newBuilder(iface);
            if (svcName != null) {
                builder.withServiceName(svcName);
            }
            LitelinksServiceClient client = (LitelinksServiceClient) builder.withZookeeper(ZK).build();
            clients.add(client);
            // create and register tprocessor using client
            final Constructor<? extends TProcessor> constr = getTProcConstructorFromIface(iface);
            if (constr == null) {
                throw new InvalidThriftClassException(svcClass.getName());
            }
            processor.registerProcessor(svcName, constr.newInstance(client));
        }
        if (clients.isEmpty()) {
            throw new Exception("No services to proxy found in file " + configFile);
        }
        return processor;
    }

    @Override
    protected void shutdown() {
        //TODO close clients when supported
    }
}
