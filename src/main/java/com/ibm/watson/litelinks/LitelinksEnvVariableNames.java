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

import com.ibm.watson.zk.ZookeeperClient;

/**
 * Environment variables recognized by litelinks.
 * <p>
 * NOTE:
 * SSL-related env vars (LITELINKS_SSL_*) are defined in the {@link SSLHelper} class.
 */
public /*final*/ class LitelinksEnvVariableNames {

    protected /*private*/ LitelinksEnvVariableNames() {}

    // server-side
    public static final String PORT = "WATSON_SERVICE_PORT";
    public static final String PUBLIC_PORT = "WATSON_SERVICE_REG_PORT";
    public static final String ADDRESS = "WATSON_SERVICE_ADDRESS";
    public static final String PRIVATE_ENDPOINT = "LL_PRIVATE_ENDPOINT"; //beta
    public static final String TERMINATION_MSG_PATH = "LL_TERMINATION_MSG_PATH";

    /**
     * @see LitelinksSystemPropNames#SERVER_REGISTRY
     */
    public static final String SERVER_REGISTRY = "LL_REGISTRY";

    // client-side
    public static final String MULTIPLEXER = "WATSON_SERVICE_MULTIPLEXER";
    public static final String USE_PRIVATE_ENDPOINTS = "LL_USE_PRIVATE"; //beta

    /**
     * @see LitelinksSystemPropNames#CLIENT_REGISTRIES
     */
    public static final String CLIENT_DISCOVERY = "LL_DISCOVERY";

    // both
    /**
     * @deprecated use {@link com.ibm.watson.zk.ZookeeperClient#ZK_CONN_STRING_ENV_VAR}
     */
    @Deprecated
    public static final String ZOOKEEPER_CONNECT_STRING = ZookeeperClient.ZK_CONN_STRING_ENV_VAR;

    public static final String PRIVATE_DOMAIN_ID = "LL_PRIVATE_DOMAIN_ID"; //beta

    /**
     * @see LitelinksSystemPropNames#SERVICE_CLASS_ALIASES
     */
    public static final String SERVICE_CLASS_ALIASES = "LL_SERVICE_CLASS_ALIASES";
}
