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

package com.netflix.curator.x.discovery.contexts;

import com.google.common.collect.Maps;
import com.netflix.curator.x.discovery.ServiceDiscovery;
import com.netflix.curator.x.discovery.rest.DiscoveryContext;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import javax.ws.rs.ext.ContextResolver;
import java.util.Iterator;
import java.util.Map;

/**
 * For convenience, a version of {@link DiscoveryContext} that uses a String-to-String map as the
 * payload
 */
public class MapDiscoveryContext implements DiscoveryContext<Map<String, String>>, ContextResolver<DiscoveryContext<Map<String, String>>>
{
    private final ServiceDiscovery<Map<String, String>> serviceDiscovery;
    private final int instanceRefreshMs;

    public MapDiscoveryContext(ServiceDiscovery<Map<String, String>> serviceDiscovery, int instanceRefreshMs)
    {
        this.serviceDiscovery = serviceDiscovery;
        this.instanceRefreshMs = instanceRefreshMs;
    }

    @Override
    public int getInstanceRefreshMs()
    {
        return instanceRefreshMs;
    }

    @Override
    public ServiceDiscovery<Map<String, String>> getServiceDiscovery()
    {
        return serviceDiscovery;
    }

    @Override
    public void marshallJson(ObjectNode node, String fieldName, Map<String, String> map) throws Exception
    {
        if ( map == null )
        {
            map = Maps.newHashMap();
        }
        ObjectNode objectNode = node.putObject(fieldName);
        for ( Map.Entry<String, String> entry : map.entrySet() )
        {
            objectNode.put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public Map<String, String> unMarshallJson(JsonNode node) throws Exception
    {
        Map<String, String>                     map = Maps.newHashMap();
        Iterator<Map.Entry<String,JsonNode>> fields = node.getFields();
        while ( fields.hasNext() )
        {
            Map.Entry<String, JsonNode> entry = fields.next();
            map.put(entry.getKey(), entry.getValue().asText());
        }
        return map;
    }

    @Override
    public DiscoveryContext<Map<String, String>> getContext(Class<?> type)
    {
        return this;
    }
}
