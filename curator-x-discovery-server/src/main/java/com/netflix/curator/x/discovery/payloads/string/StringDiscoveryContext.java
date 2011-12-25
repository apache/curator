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

package com.netflix.curator.x.discovery.payloads.string;

import com.netflix.curator.x.discovery.ServiceInstance;
import com.netflix.curator.x.discovery.config.DiscoveryContext;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import javax.ws.rs.ext.ContextResolver;

public abstract class StringDiscoveryContext implements DiscoveryContext<String>, ContextResolver<DiscoveryContext<String>>
{
    @Override
    public void marshallJson(ObjectNode node, String fieldName, ServiceInstance<String> serviceInstance) throws Exception
    {
        if ( serviceInstance.getPayload() != null )
        {
            node.put(fieldName, serviceInstance.getPayload());
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
