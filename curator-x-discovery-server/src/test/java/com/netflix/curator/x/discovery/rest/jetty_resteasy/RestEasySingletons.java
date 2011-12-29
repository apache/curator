/*
 *
 *  Copyright 2011 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package com.netflix.curator.x.discovery.rest.jetty_resteasy;

import com.netflix.curator.x.discovery.ServiceDiscovery;
import com.netflix.curator.x.discovery.entity.JsonServiceInstanceMarshaller;
import com.netflix.curator.x.discovery.entity.JsonServiceInstancesMarshaller;
import com.netflix.curator.x.discovery.entity.JsonServiceNamesMarshaller;
import com.netflix.curator.x.discovery.rest.mocks.MockServiceDiscovery;
import com.netflix.curator.x.discovery.contexts.StringDiscoveryContext;

/**
 * For testing purposes only. You will inject these however is appropriate for your application
 */
public class RestEasySingletons
{
    public final ServiceDiscovery<String> serviceDiscoverySingleton = new MockServiceDiscovery<String>();
    public final StringDiscoveryContext contextSingleton = new StringDiscoveryContext(serviceDiscoverySingleton, 1000);
    public final JsonServiceInstanceMarshaller<String> serviceInstanceMarshallerSingleton = new JsonServiceInstanceMarshaller<String>(contextSingleton);
    public final JsonServiceInstancesMarshaller<String> serviceInstancesMarshallerSingleton = new JsonServiceInstancesMarshaller<String>(contextSingleton);
    public final JsonServiceNamesMarshaller serviceNamesMarshallerSingleton = new JsonServiceNamesMarshaller();
}
