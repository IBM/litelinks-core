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

/**
 * A policy for how to distribute requests to available service instances.
 * The {@link #getLoadBalancer()} method will be called multiple times
 * per logical service, to create load balancers for each service instance
 * state - currently active and failing.
 * <p>
 * There are three out of the box policies: {@link #RANDOM}, {@link #ROUND_ROBIN}
 * and {@link #BALANCED}.
 * <p>
 * It's recommended to create custom LoadBalancers by subclassing
 * {@link CustomLoadBalancer} rather than implementing the {@link LoadBalancer}
 * interface directly.
 * <p>
 * Most {@link LoadBalancingPolicy}s should implement the marker subinterface
 * {@link InclusiveLoadBalancingPolicy} which indicates that one of the
 * service instances offered to {@link LoadBalancer}s will <i>always</i> be
 * selected (rather than rejecting all by returning null).
 * <p>
 * <b>NOTE:</b> custom load balancers are still "alpha" and the corresponding
 * APIs are subject to change in future versions.
 */
public interface LoadBalancingPolicy {

    LoadBalancingPolicy RANDOM = new InclusiveLoadBalancingPolicy() {
        @Override
        public LoadBalancer getLoadBalancer() {
            return LoadBalancer.RANDOM;
        }
    },
            ROUND_ROBIN = new InclusiveLoadBalancingPolicy() {
                @Override
                public LoadBalancer getLoadBalancer() {
                    return new LoadBalancer.RoundRobin();
                }
            },
            BALANCED = new InclusiveLoadBalancingPolicy() {
                @Override
                public LoadBalancer getLoadBalancer() {
                    return LoadBalancer.BALANCED;
                }
            };

    /**
     * @return a {@link LoadBalancer} instance corresponding to this policy
     */
    LoadBalancer getLoadBalancer();

    /**
     * Marker interface which should be implemented by policies which will
     * always choose an offered service instance rather than ever rejecting
     * all by returning null
     */
    interface InclusiveLoadBalancingPolicy extends LoadBalancingPolicy {
    }
}
