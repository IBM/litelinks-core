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
package com.ibm.watson.litelinks.etcd;

import com.google.protobuf.ByteString;
import com.ibm.etcd.client.KeyUtils;

public class EtcdDiscovery {

    private EtcdDiscovery() {}

    public static final ByteString LITELINKS_PREFIX =
            ByteString.copyFromUtf8("/litelinks/");

    public static ByteString litelinksPrefix(ByteString namespace) {
        return namespace == null? LITELINKS_PREFIX : namespace.concat(LITELINKS_PREFIX);
    }

    public static ByteString servicePrefix(ByteString litelinksPrefix, String serviceName) {
        return litelinksPrefix.concat(ByteString.copyFromUtf8(serviceName)
                .concat(KeyUtils.singleByte('/')));
    }

    public static ByteString instanceKey(ByteString namespace, String serviceName, String instanceUid) {
        return servicePrefix(litelinksPrefix(namespace), serviceName)
                .concat(ByteString.copyFromUtf8(instanceUid));
    }

}
