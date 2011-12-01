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

package com.netflix.curator.x.discovery;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.curator.x.discovery.details.ServiceDiscoveryImpl;
import com.netflix.curator.x.discovery.strategies.RoundRobinStrategy;
import java.util.concurrent.ThreadFactory;

/**
 * Builder for service providers
 */
public class ServiceProviderBuilder<T>
{
    private ServiceDiscovery<T> discovery;
    private String serviceName;
    private ProviderStrategy<T> providerStrategy;
    private ThreadFactory threadFactory;
    private int refreshPaddingMs;

    /**
     * Allocate a new builder. {@link #providerStrategy} is set to {@link RoundRobinStrategy}
     * and {@link #refreshPaddingMs} is set to 1 second.
     *
     * @return the builder
     */
    public static<T> ServiceProviderBuilder<T>  builder()
    {
        return new ServiceProviderBuilder<T>();
    }

    /**
     * Allocate a new service provider based on the current builder settings
     *
     * @return provider
     */
    public ServiceProvider<T> build()
    {
        return new ServiceProvider<T>(discovery, serviceName, providerStrategy, threadFactory, refreshPaddingMs);
    }

    /**
     * required - set the discovery instance for the provider
     *
     * @param discovery discovery to use
     * @return this
     */
    public ServiceProviderBuilder<T> discovery(ServiceDiscovery<T> discovery)
    {
        this.discovery = discovery;
        return this;
    }

    /**
     * required - set the name of the service to be provided
     *
     * @param serviceName the name of the service
     * @return this
     */
    public ServiceProviderBuilder<T> serviceName(String serviceName)
    {
        this.serviceName = serviceName;
        return this;
    }

    /**
     * optional - set the provider strategy. The default is {@link RoundRobinStrategy}
     *
     * @param providerStrategy strategy to use
     * @return this
     */
    public ServiceProviderBuilder<T> providerStrategy(ProviderStrategy<T> providerStrategy)
    {
        this.providerStrategy = providerStrategy;
        return this;
    }

    /**
     * optional - the thread factory to use for creating internal threads
     *
     * @param threadFactory factory to use
     * @return this
     */
    public ServiceProviderBuilder<T> threadFactory(ThreadFactory threadFactory)
    {
        this.threadFactory = threadFactory;
        return this;
    }

    /**
     * optional To avoid herding in noisy scenarios, the cache should be padded to only update 1 per period.
     * The refresh padding is that period in milliseconds. Set to 0 to turn off padding. The default
     * is 1 second.
     *
     * @param refreshPaddingMs padding in milliseconds
     * @return this
     */
    public ServiceProviderBuilder<T> refreshPaddingMs(int refreshPaddingMs)
    {
        this.refreshPaddingMs = refreshPaddingMs;
        return this;
    }

    ServiceProviderBuilder()
    {
        providerStrategy = new RoundRobinStrategy<T>();
        threadFactory = new ThreadFactoryBuilder().setNameFormat("ServiceProvider-%d").build();
        refreshPaddingMs = ServiceDiscoveryImpl.DEFAULT_REFRESH_PADDING;
    }
}
