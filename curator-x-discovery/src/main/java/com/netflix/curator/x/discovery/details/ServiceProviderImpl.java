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

package com.netflix.curator.x.discovery.details;

import com.netflix.curator.x.discovery.ProviderStrategy;
import com.netflix.curator.x.discovery.ServiceInstance;
import com.netflix.curator.x.discovery.ServiceProvider;
import java.io.IOException;
import java.util.concurrent.ThreadFactory;

/**
 * The main interface for Service Discovery. Encapsulates the discovery service for a particular
 * named service along with a provider strategy. 
 */
public class ServiceProviderImpl<T> implements ServiceProvider<T>
{
    private final ServiceCache<T> cache;
    private final ServiceDiscoveryImpl<T> discovery;
    private final ProviderStrategy<T> providerStrategy;

    public ServiceProviderImpl(ServiceDiscoveryImpl<T> discovery, String serviceName, ProviderStrategy<T> providerStrategy, ThreadFactory threadFactory, int refreshPaddingMs)
    {
        this.discovery = discovery;
        this.providerStrategy = providerStrategy;
        cache = discovery.serviceCacheBuilder().name(serviceName).refreshPaddingMs(refreshPaddingMs).threadFactory(threadFactory).build();
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
     * Return an instance for a single use. <b>IMPORTANT: </b> users
     * should not hold on to the instance returned. They should always get a fresh instance.
     *
     * @return the instance to use
     * @throws Exception any errors
     */
    @Override
    public ServiceInstance<T> getInstance() throws Exception
    {
        return providerStrategy.getInstance(cache);
    }
}
