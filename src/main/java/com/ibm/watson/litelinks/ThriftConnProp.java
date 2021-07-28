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

import org.slf4j.Logger;

import java.util.Map;
import java.util.Map.Entry;

public abstract class ThriftConnProp {

    private ThriftConnProp() {}

    public static final String TR_PROTO_FACTORY = "transport.tprotocol.factory";
    public static final String TR_FRAMED = "transport.framed";
    public static final String TR_SSL = "transport.ssl.enabled";
    public static final String TR_SSL_PROTOCOL = "transport.ssl.protocol";
    public static final String TR_EXTRA_INFO = "transport.extrainfo_supported";

    public static final String SERVICE_CLASS = "service.class";

    public static final String PRIVATE_ENDPOINT = "PRIVATE_ENDPOINT";

    public static final String MULTIPLEX_CLASS = "#MULTIPLEXED#";

    public static final String METH_INFO_PREFIX = "methodinfo.";
    public static final String APP_METADATA_PREFIX = "app.";

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
