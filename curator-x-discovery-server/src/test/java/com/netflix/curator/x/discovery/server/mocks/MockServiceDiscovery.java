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

package com.netflix.curator.x.discovery.server.mocks;

import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.netflix.curator.x.discovery.ServiceCacheBuilder;
import com.netflix.curator.x.discovery.ServiceDiscovery;
import com.netflix.curator.x.discovery.ServiceInstance;
import com.netflix.curator.x.discovery.ServiceProviderBuilder;
import java.io.IOException;
import java.util.Collection;

public class MockServiceDiscovery<T> implements ServiceDiscovery<T>
{
    private final Multimap<String, ServiceInstance<T>>      services = Multimaps.synchronizedMultimap
    (
        Multimaps.newMultimap
        (
            Maps.<String, Collection<ServiceInstance<T>>>newHashMap(),
            new Supplier<Collection<ServiceInstance<T>>>()
            {
                @Override
                public Collection<ServiceInstance<T>> get()
                {
                    return Lists.newArrayList();
                }
            }
        )
    );

    @Override
    public void start() throws Exception
    {
    }

    @Override
    public void registerService(ServiceInstance<T> service) throws Exception
    {
        services.put(service.getName(), service);
    }

    @Override
    public void unregisterService(ServiceInstance<T> service) throws Exception
    {
        services.remove(service.getName(), service);
    }

    @Override
    public ServiceCacheBuilder<T> serviceCacheBuilder()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<String> queryForNames() throws Exception
    {
        return services.keys();
    }

    @Override
    public Collection<ServiceInstance<T>> queryForInstances(String name) throws Exception
    {
        return services.get(name);
    }

    @Override
    public ServiceInstance<T> queryForInstance(String name, String id) throws Exception
    {
        Collection<ServiceInstance<T>> instances = services.get(name);
        for ( ServiceInstance<T> instance : instances )
        {
            if ( instance.getId().equals(id) )
            {
                return instance;
            }
        }
        return null;
    }

    @Override
    public ServiceProviderBuilder<T> serviceProviderBuilder()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException
    {
    }
}
