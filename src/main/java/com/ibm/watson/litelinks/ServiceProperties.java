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
package com.ibm.watson.litelinks;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;

public final class ServiceProperties {
    private static final Logger logger = LoggerFactory.getLogger(ServiceProperties.class);

    private ServiceProperties() {}

    // Thrift-specific
    public static final String TR_PROTO_FACTORY = "transport.tprotocol.factory";
    public static final String TR_FRAMED = "transport.framed";
    // Other connection properties
    public static final String TR_SSL = "transport.ssl.enabled";
    public static final String TR_SSL_PROTOCOL = "transport.ssl.protocol";
    public static final String TR_EXTRA_INFO = "transport.extrainfo_supported";

    public static final String SERVICE_CLASS = "service.class";

    public static final String PRIVATE_ENDPOINT = "PRIVATE_ENDPOINT";

    public static final String MULTIPLEX_CLASS = "#MULTIPLEXED#";

    public static final String METH_INFO_PREFIX = "methodinfo.";
    public static final String APP_METADATA_PREFIX = "app.";

    public static final Map<String, String> SERVICE_CLASS_ALIASES;

    static {
        String serviceClassAliasConfig = System.getProperty(LitelinksSystemPropNames.SERVICE_CLASS_ALIASES);
        if (serviceClassAliasConfig == null) {
            serviceClassAliasConfig = System.getenv(LitelinksEnvVariableNames.SERVICE_CLASS_ALIASES);
        }
        ImmutableMap.Builder<String, String> builder = null;
        if (serviceClassAliasConfig != null) {
            for (String entry : serviceClassAliasConfig.split(",")) {
                String[] values = entry.split("=");
                if (values.length != 2) {
                    throw new RuntimeException("Invalid value for " + LitelinksEnvVariableNames.SERVICE_CLASS_ALIASES
                            + " env var: " + serviceClassAliasConfig);
                }
                if (builder == null) {
                    builder = ImmutableMap.builder();
                }
                builder.put(values[0], values[1]);
            }
        }
        SERVICE_CLASS_ALIASES = builder != null ? builder.build() : Collections.emptyMap();
        if (SERVICE_CLASS_ALIASES != null) {
            for (Entry<String, String> ent : SERVICE_CLASS_ALIASES.entrySet()) {
                logger.info("Configured litelinks service class name alias: " + ent.getKey() + " -> " + ent.getValue());
            }
        }
    }

    public static void log(Logger logger, Map<Object, Object> props) {
        if (props != null && !props.isEmpty()) {
            for (Entry<Object, Object> ent : props.entrySet()) {
                logger.debug("  " + ent.getKey() + "=" + ent.getValue());
            }
        } else {
            logger.debug("  (none)");
        }
    }
}
