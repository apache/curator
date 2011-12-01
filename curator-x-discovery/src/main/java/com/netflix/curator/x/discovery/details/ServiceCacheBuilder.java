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

import java.util.concurrent.ThreadFactory;

/**
 * Builder for a service cache
 */
public class ServiceCacheBuilder<T>
{
    private ServiceDiscoveryImpl<T> discovery;
    private String name;
    private ThreadFactory threadFactory;
    private int refreshPaddingMs;
    
    ServiceCacheBuilder(ServiceDiscoveryImpl<T> discovery)
    {
        this.discovery = discovery;
    }

    /**
     * Return a new service cache with the current settings
     *
     * @return service cache
     */
    public ServiceCache<T>  build()
    {
        return new ServiceCache<T>(discovery, name, threadFactory, refreshPaddingMs);
    }

    /**
     * The name of the service to cache (required)
     *
     * @param name service name
     * @return this
     */
    public ServiceCacheBuilder<T> name(String name)
    {
        this.name = name;
        return this;
    }

    /**
     * Optional thread factory to use for the cache's internal thread
     *
     * @param threadFactory factory
     * @return this
     */
    public ServiceCacheBuilder<T> threadFactory(ThreadFactory threadFactory)
    {
        this.threadFactory = threadFactory;
        return this;
    }

    /**
     * To avoid herding in noisy scenarios, the cache should be padded to only update 1 per period.
     * The refresh padding is that period in milliseconds. Set to 0 to turn off padding.
     *
     * @param refreshPaddingMs padding in milliseconds
     * @return this
     */
    public ServiceCacheBuilder<T> refreshPaddingMs(int refreshPaddingMs)
    {
        this.refreshPaddingMs = refreshPaddingMs;
        return this;
    }
}
