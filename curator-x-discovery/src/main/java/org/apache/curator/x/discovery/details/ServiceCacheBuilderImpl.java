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

import org.apache.curator.utils.CloseableExecutorService;
import org.apache.curator.x.discovery.ServiceCache;
import org.apache.curator.x.discovery.ServiceCacheBuilder;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;

/**
 * Builder for a service cache
 */
class ServiceCacheBuilderImpl<T> implements ServiceCacheBuilder<T>
{
    private ServiceDiscoveryImpl<T> discovery;
    private String name;
    private ThreadFactory threadFactory;
    private CloseableExecutorService executorService;

    ServiceCacheBuilderImpl(ServiceDiscoveryImpl<T> discovery)
    {
        this.discovery = discovery;
    }

    /**
     * Return a new service cache with the current settings
     *
     * @return service cache
     */
    @Override
    public ServiceCache<T> build()
    {
        if (executorService != null)
        {
            return new ServiceCacheImpl<T>(discovery, name, executorService);
        }
        else
        {
            return new ServiceCacheImpl<T>(discovery, name, threadFactory);
        }
    }

    /**
     * The name of the service to cache (required)
     *
     * @param name service name
     * @return this
     */
    @Override
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
    @Override
    @Deprecated
    public ServiceCacheBuilder<T> threadFactory(ThreadFactory threadFactory)
    {
        this.threadFactory = threadFactory;
        this.executorService = null;
        return this;
    }

    /**
     * Optional executor service to use for the cache's background thread
     *
     * @param executorService executor service
     * @return this
     */
    @Override
    public ServiceCacheBuilder<T> executorService(ExecutorService executorService) {
        return executorService(new CloseableExecutorService(executorService, false));
    }

    /**
     * Optional CloseableExecutorService to use for the cache's background thread
     *
     * @param executorService an instance of CloseableExecutorService
     * @return this
     */
    @Override
    public ServiceCacheBuilder<T> executorService(CloseableExecutorService executorService) {
        this.executorService = executorService;
        this.threadFactory = null;
        return this;
    }
}
