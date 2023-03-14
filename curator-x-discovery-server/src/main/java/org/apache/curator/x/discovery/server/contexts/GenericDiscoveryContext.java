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

package org.apache.curator.x.discovery.server.contexts;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.reflect.TypeToken;
import org.apache.curator.x.discovery.ProviderStrategy;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.server.rest.DiscoveryContext;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;

/**
 * For convenience, a version of {@link DiscoveryContext} that uses any generic type as the payload
 */
@Provider
public class GenericDiscoveryContext<T> implements DiscoveryContext<T>, ContextResolver<DiscoveryContext<T>>
{
    private final ServiceDiscovery<T> serviceDiscovery;
    private final ProviderStrategy<T> providerStrategy;
    private final int instanceRefreshMs;
    private final TypeToken<T> payloadType;

    public GenericDiscoveryContext(ServiceDiscovery<T> serviceDiscovery, ProviderStrategy<T> providerStrategy, int instanceRefreshMs, Class<T> payloadType)
    {
        this(serviceDiscovery, providerStrategy, instanceRefreshMs, TypeToken.of(payloadType));
    }

    public GenericDiscoveryContext(ServiceDiscovery<T> serviceDiscovery, ProviderStrategy<T> providerStrategy, int instanceRefreshMs, TypeToken<T> payloadType)
    {
        this.serviceDiscovery = serviceDiscovery;
        this.providerStrategy = providerStrategy;
        this.instanceRefreshMs = instanceRefreshMs;
        this.payloadType = payloadType;
    }

    @Override
    public ProviderStrategy<T> getProviderStrategy()
    {
        return providerStrategy;
    }

    @Override
    public int getInstanceRefreshMs()
    {
        return instanceRefreshMs;
    }

    @Override
    public ServiceDiscovery<T> getServiceDiscovery()
    {
        return serviceDiscovery;
    }

	@Override
    public void marshallJson(ObjectNode node, String fieldName, T payload) throws Exception
    {
        if ( payload == null )
        {
            //noinspection unchecked
            payload = (T)payloadType.getRawType().newInstance();
        }
        
        node.putPOJO(fieldName, payload);
    }

	@Override
    public T unMarshallJson(JsonNode node) throws Exception
    {
        T payload;
        ObjectMapper mapper = new ObjectMapper();
        //noinspection unchecked
        payload = (T)mapper.readValue(node.toString(), payloadType.getRawType());
        return payload;
    }

    @Override
    public DiscoveryContext<T> getContext(Class<?> type)
    {
        return this;
    }
}
