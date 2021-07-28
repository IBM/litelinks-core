/*
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

import java.util.List;

/**
 * Listener interface used by clients to receive notifications related to changes
 * in a service's availability.
 */
public interface AvailabilityListener {

    /**
     * Called when the availability of the service changes, i.e. from
     * zero instances to/from n instances, where n &gt;= 1.
     * {@link #instancesChanged(String, List)} will always be called with this,
     * but the converse is not necessarily true.
     *
     * @param serviceName name of the service
     * @param available   true if the service is available, false otherwise
     */
    void availabilityChanged(String serviceName, boolean available);

    /**
     * Called whenever there is a change to the list and/or states of known instances.
     * This could be either as a result of the instances being registered/deregistered,
     * or due to the state of one or more existing instances changing - for example
     * moving to FAILING state as a result of invocation connection failures.
     *
     * @param serviceName   name of the service
     * @param instanceInfos current list of known instances
     */
    void instancesChanged(String serviceName, List<ServiceInstanceInfo> instanceInfos);

}
