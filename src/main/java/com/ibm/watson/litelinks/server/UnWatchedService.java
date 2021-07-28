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

import com.google.common.util.concurrent.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pass-through service registry wrapper, for "standalone" service
 * instances which don't register anywhere
 */
public class UnWatchedService extends WatchedService {

    private static final Logger logger = LoggerFactory.getLogger(WatchedService.class);

    public UnWatchedService(Service service, String serviceName,
            String serviceVersion, String instanceId, int port, int probePort) {
        super(service, serviceName, serviceVersion, instanceId, port, probePort);
    }

    @Override
    protected boolean deregister() {
        return false;
    }

    @Override
    protected void registerAsync() {
        logger.info("No service registry configured for service " + getServiceName());
        notifyStarted();
    }

    @Override
    protected boolean isRegistered() {
        return false;
    }
}
