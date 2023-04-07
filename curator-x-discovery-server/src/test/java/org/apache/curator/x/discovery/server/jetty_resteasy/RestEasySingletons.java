/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.curator.x.discovery.server.jetty_resteasy;

import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.server.entity.JsonServiceInstanceMarshaller;
import org.apache.curator.x.discovery.server.entity.JsonServiceInstancesMarshaller;
import org.apache.curator.x.discovery.server.entity.JsonServiceNamesMarshaller;
import org.apache.curator.x.discovery.server.mocks.MockServiceDiscovery;
import org.apache.curator.x.discovery.server.contexts.StringDiscoveryContext;
import org.apache.curator.x.discovery.strategies.RandomStrategy;

/**
 * For testing purposes only. You will inject these however is appropriate for your application
 */
public class RestEasySingletons
{
    public final ServiceDiscovery<String> serviceDiscoverySingleton = new MockServiceDiscovery<String>();
    public final StringDiscoveryContext contextSingleton = new StringDiscoveryContext(serviceDiscoverySingleton, new RandomStrategy<String>(), 1000);
    public final JsonServiceInstanceMarshaller<String> serviceInstanceMarshallerSingleton = new JsonServiceInstanceMarshaller<String>(contextSingleton);
    public final JsonServiceInstancesMarshaller<String> serviceInstancesMarshallerSingleton = new JsonServiceInstancesMarshaller<String>(contextSingleton);
    public final JsonServiceNamesMarshaller serviceNamesMarshallerSingleton = new JsonServiceNamesMarshaller();
}
