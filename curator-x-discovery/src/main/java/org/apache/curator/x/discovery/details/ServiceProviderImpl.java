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
package org.apache.curator.x.discovery.details;

import com.google.common.collect.Lists;
import org.apache.curator.x.discovery.DownInstancePolicy;
import org.apache.curator.x.discovery.InstanceFilter;
import org.apache.curator.x.discovery.ProviderStrategy;
import org.apache.curator.x.discovery.ServiceCache;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceProvider;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ThreadFactory;

/**
 * The main interface for Service Discovery. Encapsulates the discovery service for a particular
 * named service along with a provider strategy. 
 */
public class ServiceProviderImpl<T> implements ServiceProvider<T>
{
    private final ServiceCache<T> cache;
    private final InstanceProvider<T> instanceProvider;
    private final ServiceDiscoveryImpl<T> discovery;
    private final ProviderStrategy<T> providerStrategy;
    private final DownInstanceManager<T> downInstanceManager;

    public ServiceProviderImpl(ServiceDiscoveryImpl<T> discovery, String serviceName, ProviderStrategy<T> providerStrategy, ThreadFactory threadFactory, List<InstanceFilter<T>> filters, DownInstancePolicy downInstancePolicy)
    {
        this.discovery = discovery;
        this.providerStrategy = providerStrategy;

        downInstanceManager = new DownInstanceManager<T>(downInstancePolicy);
        cache = discovery.serviceCacheBuilder().name(serviceName).threadFactory(threadFactory).build();

        ArrayList<InstanceFilter<T>> localFilters = Lists.newArrayList(filters);
        localFilters.add(downInstanceManager);
        localFilters.add(new InstanceFilter<T>()
        {
            @Override
            public boolean apply(ServiceInstance<T> instance)
            {
                return instance.isEnabled();
            }
        });
        instanceProvider = new FilteredInstanceProvider<T>(cache, localFilters);
    }

    /**
     * The provider must be started before use
     *
     * @throws Exception any errors
     */
    @Override
    public void start() throws Exception
    {
        cache.start();
        discovery.providerOpened(this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException
    {
        discovery.providerClosed(this);
        cache.close();
    }

    /**
     * Return the current available set of instances <b>IMPORTANT: </b> users
     * should not hold on to the instance returned. They should always get a fresh list.
     *
     * @return all known instances
     * @throws Exception any errors
     */
    @Override
    public Collection<ServiceInstance<T>> getAllInstances() throws Exception
    {
        return instanceProvider.getInstances();
    }

    /**
     * Return an instance for a single use. <b>IMPORTANT: </b> users
     * should not hold on to the instance returned. They should always get a fresh instance.
     *
     * @return the instance to use
     * @throws Exception any errors
     */
    @Override
    public ServiceInstance<T> getInstance() throws Exception
    {
        return providerStrategy.getInstance(instanceProvider);
    }

    @Override
    public void noteError(ServiceInstance<T> instance)
    {
        downInstanceManager.add(instance);
    }
}
