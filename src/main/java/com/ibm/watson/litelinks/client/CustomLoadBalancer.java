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

import com.ibm.watson.litelinks.client.LitelinksServiceClient.ServiceInstanceInfo;
import com.ibm.watson.litelinks.client.LoadBalancingPolicy.InclusiveLoadBalancingPolicy;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * This class should be used to create {@link LoadBalancer}s in preference
 * to implementing {@link LoadBalancer} directly.
 */
@SuppressWarnings("unchecked")
public abstract class CustomLoadBalancer implements LoadBalancer {

    static class ListHolder {
        final List<ServiceInstanceInfo> list;
        final Object[] arr;

        @SuppressWarnings("rawtypes")
        public ListHolder(Object[] arr) {
            this.arr = arr;
            this.list = (List<ServiceInstanceInfo>)(List) Collections.unmodifiableList(Arrays.asList(arr));
        }
    }

    ListHolder holder = new ListHolder(new Object[0]);

    @Override
    public final <T> T getNext(Object[] list, String method, Object[] args) {
        ListHolder lh = holder;
        if (lh.arr != list) {
            holder = lh = new ListHolder(list);
        }
        return (T) getNext(lh.list, method, args);
    }

    /**
     * Choose a service instance to send a request to from a provided list. The order of service instances
     * in the list passed to a given LoadBalancer will remain constant. If the corresponding
     * {@link LoadBalancingPolicy} implements {@link InclusiveLoadBalancingPolicy} then one of the provided
     * service instance <i>must</i> be returned. Otherwise, null <i>may</i> be returned to reject all of the
     * the provided instances.
     *
     * @param list   the list of service instances to select from
     * @param method the method being invoked, could be null in the case of connection-test requests
     * @param args   the arguments passed to the method, or null in the case of connection-test requests
     * @return a service instance from the list passed to this method
     */
    public abstract ServiceInstanceInfo getNext(List<ServiceInstanceInfo> list, String method, Object[] args);
}
