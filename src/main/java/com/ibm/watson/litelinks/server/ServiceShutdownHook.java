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

import com.ibm.watson.kvutils.OrderedShutdownHooks;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class ServiceShutdownHook {

    //private static final Logger logger = LoggerFactory.getLogger(ServiceShutdownHook.class);

    private ServiceShutdownHook() {} // static only

    private static final List<LitelinksService> servicesToStop;

    public static void registerForShutdown(LitelinksService svc) {
        if (servicesToStop != null) {
            servicesToStop.add(svc);
        }
    }

    public static void removeShutdownRegistration(LitelinksService svc) {
        if (servicesToStop != null) {
            servicesToStop.remove(svc);
        }
    }

    static {
        servicesToStop = new CopyOnWriteArrayList<>();
        final long timeout = LitelinksService.getSignalShutdownTimeoutMs();
        OrderedShutdownHooks.addHook(100, () -> {
            if (servicesToStop != null) {
                List<LitelinksService> stopped = null;
                if (!servicesToStop.isEmpty()) {
                    stopped = new ArrayList<>(servicesToStop.size());
                    for (LitelinksService s : servicesToStop) {
                        stopped.add(s.stopWithDeadline(timeout));
                    }
                    try {
                        for (LitelinksService s : stopped) {
                            s.awaitStopped(timeout);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        });
    }
}
