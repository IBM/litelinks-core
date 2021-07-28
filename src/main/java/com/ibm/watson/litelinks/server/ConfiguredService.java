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

import java.util.Map;

public interface ConfiguredService {

    /**
     * Configuration used by clients to connect to this service
     * (e.g. protocol, encryption, API version, etc)
     *
     * @return the config data in an arbitrary format, or null
     * @throws Exception if the service-provided config is erroneous or inconsistent
     */
    Map<String, String> getConfig() throws Exception;

    /**
     * Verify that the supplied configuration is equivalent/compatible
     * with this service's configuration. i.e. ensure this service can
     * belong to a cluster whose other services have targetConfig.
     *
     * @param targetConfig the config to compare with this service's config
     * @throws ConfigMismatchException if this service's config is not compatible
     */
    void verifyConfig(Map<String, String> targetConfig) throws ConfigMismatchException;


    class ConfigMismatchException extends Exception {

        private static final long serialVersionUID = 1L;

        public ConfigMismatchException() {
			super();
        }

        public ConfigMismatchException(String message) {
            super(message);
        }

        public ConfigMismatchException(Throwable cause) {
            super(cause);
        }

    }
}
