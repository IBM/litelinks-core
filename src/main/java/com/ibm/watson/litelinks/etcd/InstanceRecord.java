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

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.ibm.watson.kvutils.JsonSerializer;
import com.ibm.watson.litelinks.client.ServiceRegistryClient;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

public class InstanceRecord extends ServiceRegistryClient.Listener.Server {

    public static final JsonSerializer<InstanceRecord> SERIALIZER =
            new JsonSerializer<>(InstanceRecord.class);

    // for forwards-compatibility
    private Map<String, Object> unknownAttrs;

    @JsonAnyGetter
    public Map<String, Object> getUnknown() {
        Map<String, Object> unknown = unknownAttrs;
        return unknown != null? unknown : Collections.emptyMap();
    }

    @JsonAnySetter
    public void setUnknown(String name, Object value) {
        // TreeMap because expected to be small
        if (unknownAttrs == null) {
            unknownAttrs = new TreeMap<>();
        }
        unknownAttrs.put(name, value);
    }

    public InstanceRecord(String hostname, int port,
            long registrationTime, String version, String key,
            String instanceId, Map<Object, Object> connConfig) {
        super(hostname, port, registrationTime, version, key, instanceId, connConfig);
    }

    @JsonIgnore
    public String getKey() {
        return key;
    }

    // for jackson defaults comparison
    public InstanceRecord() {
        this(null, 0, 0L, null, null, null, Collections.emptyMap());
    }
}
