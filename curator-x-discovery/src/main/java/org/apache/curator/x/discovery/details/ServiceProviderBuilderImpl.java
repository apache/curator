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

package org.apache.curator.x.discovery.details;

import com.google.common.collect.Lists;
import org.apache.curator.x.discovery.DownInstancePolicy;
import org.apache.curator.x.discovery.InstanceFilter;
import org.apache.curator.x.discovery.ProviderStrategy;
import org.apache.curator.x.discovery.ServiceProvider;
import org.apache.curator.x.discovery.ServiceProviderBuilder;
import org.apache.curator.x.discovery.strategies.RoundRobinStrategy;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;

/**
 * Builder for service providers
 */
class ServiceProviderBuilderImpl<T> implements ServiceProviderBuilder<T>
{
    private ServiceDiscoveryImpl<T> discovery;
    private String serviceName;
    private ProviderStrategy<T> providerStrategy;
    private ThreadFactory threadFactory;
    private ExecutorService executorService;
    private List<InstanceFilter<T>> filters = Lists.newArrayList();
    private DownInstancePolicy downInstancePolicy = new DownInstancePolicy();

    public ServiceProvider<T> build()
    {
        return new ServiceProviderImpl<T>(discovery, serviceName, providerStrategy, threadFactory, executorService, filters, downInstancePolicy);
    }

    ServiceProviderBuilderImpl(ServiceDiscoveryImpl<T> discovery)
    {
        this.discovery = discovery;
    }

    /**
     * required - set the name of the service to be provided
     *
     * @param serviceName the name of the service
     * @return this
     */
    @Override
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
    @Override
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
    @Override
    @Deprecated
    public ServiceProviderBuilder<T> threadFactory(ThreadFactory threadFactory)
    {
        this.threadFactory = threadFactory;
        this.executorService = null;
        return this;
    }

    @Override
    public ServiceProviderBuilder<T> downInstancePolicy(DownInstancePolicy downInstancePolicy)
    {
        this.downInstancePolicy = downInstancePolicy;
        return this;
    }

    @Override
    public ServiceProviderBuilder<T> additionalFilter(InstanceFilter<T> filter)
    {
        filters.add(filter);
        return this;
    }

    @Override
    public ServiceProviderBuilder<T> executorService(ExecutorService executorService)
    {
        this.executorService = executorService;
        this.threadFactory = null;
        return this;
    }
}
