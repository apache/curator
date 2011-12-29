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

package com.netflix.curator.x.discovery.server.contexts;

import com.netflix.curator.x.discovery.ProviderStrategy;
import com.netflix.curator.x.discovery.ServiceDiscovery;
import com.netflix.curator.x.discovery.server.rest.DiscoveryContext;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;

/**
 * For convenience, a version of {@link DiscoveryContext} that uses a string as the
 * payload
 */
@Provider
public class StringDiscoveryContext implements DiscoveryContext<String>, ContextResolver<DiscoveryContext<String>>
{
    private final ServiceDiscovery<String> serviceDiscovery;
    private final ProviderStrategy<String> providerStrategy;
    private final int instanceRefreshMs;

    public StringDiscoveryContext(ServiceDiscovery<String> serviceDiscovery, ProviderStrategy<String> providerStrategy, int instanceRefreshMs)
    {
        this.serviceDiscovery = serviceDiscovery;
        this.providerStrategy = providerStrategy;
        this.instanceRefreshMs = instanceRefreshMs;
    }

    @Override
    public ProviderStrategy<String> getProviderStrategy()
    {
        return providerStrategy;
    }

    @Override
    public int getInstanceRefreshMs()
    {
        return instanceRefreshMs;
    }

    @Override
    public ServiceDiscovery<String> getServiceDiscovery()
    {
        return serviceDiscovery;
    }

    @Override
    public void marshallJson(ObjectNode node, String fieldName, String payload) throws Exception
    {
        if ( payload != null )
        {
            node.put(fieldName, payload);
        }
    }

    @Override
    public String unMarshallJson(JsonNode node) throws Exception
    {
        return (node != null) ? node.asText() : null;
    }

    @Override
    public DiscoveryContext<String> getContext(Class<?> type)
    {
        return this;
    }
}
