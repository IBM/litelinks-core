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

import org.apache.thrift.TServiceClient;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * It is recommended to subclass {@link CustomLoadBalancer} rather than
 * implement this interface directly
 */
@SuppressWarnings("unchecked")
public interface LoadBalancer {

    <T> T getNext(Object[] list, String method, Object[] args);

    LoadBalancer RANDOM = new LoadBalancer() {
        @Override
        public <T> T getNext(Object[] list, String method, Object[] args) {
            return (T) list[ThreadLocalRandom.current().nextInt(list.length)];
        }
    };

    LoadBalancer BALANCED = new LoadBalancer() {
        @Override
        public <T> T getNext(Object[] list, String method, Object[] args) {
            return (T) getLeastLoaded(list, list.length);
        }

        final <C extends TServiceClient> ServiceInstance<C> getLeastLoaded(Object[] array, int len) {
            ServiceInstance<C> chosen = (ServiceInstance<C>) array[0];
            int min = chosen.getInUseCount();
            long oldest = chosen.getLastUsedTime();
            for (int i = 1; i < len; i++) {
                ServiceInstance<C> next = (ServiceInstance<C>) array[i];
                int inuse = next.getInUseCount();
                if (inuse > min) {
                    continue;
                }
                long nlu = next.getLastUsedTime();
                if (inuse < min) {
                    min = inuse;
                } else if (nlu >= oldest) {
                    continue; // inuse == min
                }
                chosen = next;
                oldest = nlu;
            }
            return chosen;
        }
    };

    class RoundRobin implements LoadBalancer {
        final AtomicInteger count = new AtomicInteger(-1);

        @Override
        public <T> T getNext(Object[] list, String method, Object[] args) {
            int next = count.incrementAndGet(), n = list.length, idx = next % n;
            if (next >= n - 1) {
                count.compareAndSet(next, idx);
            }
            return (T) list[idx];
        }
    }
}
