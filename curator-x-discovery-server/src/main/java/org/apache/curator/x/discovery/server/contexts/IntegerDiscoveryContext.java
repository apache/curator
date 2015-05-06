/**
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
package org.apache.curator.x.discovery.server.contexts;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.curator.x.discovery.ProviderStrategy;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.server.rest.DiscoveryContext;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;

/**
 * For convenience, a version of {@link DiscoveryContext} that uses an int as the
 * payload
 */
@Provider
public class IntegerDiscoveryContext implements DiscoveryContext<Integer>, ContextResolver<DiscoveryContext<Integer>>
{
    private final ServiceDiscovery<Integer> serviceDiscovery;
    private final ProviderStrategy<Integer> providerStrategy;
    private final int instanceRefreshMs;

    public IntegerDiscoveryContext(ServiceDiscovery<Integer> serviceDiscovery, ProviderStrategy<Integer> providerStrategy, int instanceRefreshMs)
    {
        this.serviceDiscovery = serviceDiscovery;
        this.providerStrategy = providerStrategy;
        this.instanceRefreshMs = instanceRefreshMs;
    }

    @Override
    public ProviderStrategy<Integer> getProviderStrategy()
    {
        return providerStrategy;
    }

    @Override
    public int getInstanceRefreshMs()
    {
        return instanceRefreshMs;
    }

    @Override
    public ServiceDiscovery<Integer> getServiceDiscovery()
    {
        return serviceDiscovery;
    }

    @Override
    public void marshallJson(ObjectNode node, String fieldName, Integer payload) throws Exception
    {
        if ( payload != null )
        {
            node.put(fieldName, payload.toString());
        }
    }

    @Override
    public Integer unMarshallJson(JsonNode node) throws Exception
    {
        if ( node != null )
        {
            return Integer.parseInt(node.asText());
        }
        return null;
    }

    @Override
    public DiscoveryContext<Integer> getContext(Class<?> type)
    {
        return this;
    }
}
