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

package com.netflix.curator.x.discovery.rest.concretes;

import com.netflix.curator.x.discovery.ServiceDiscovery;
import com.netflix.curator.x.discovery.config.DiscoveryConfig;
import com.netflix.curator.x.discovery.payloads.string.StringDiscoveryContext;
import com.netflix.curator.x.discovery.rest.mocks.MockServiceDiscovery;
import javax.ws.rs.ext.Provider;

@Provider
public class StringDiscoveryContextForTest extends StringDiscoveryContext
{
    private final ServiceDiscovery<String> discovery = new MockServiceDiscovery<String>();

    private final DiscoveryConfig config = new DiscoveryConfig()
    {
        @Override
        public int getInstanceRefreshMs()
        {
            return 1000;
        }
    };

    @Override
    public DiscoveryConfig getDiscoveryConfig()
    {
        return config;
    }

    @Override
    public ServiceDiscovery<String> getServiceDiscovery()
    {
        return discovery;
    }
}
