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

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.curator.x.discovery.DownInstanceManager;
import org.apache.curator.x.discovery.ProviderStrategy;
import org.apache.curator.x.discovery.ServiceCache;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceProvider;
import java.io.IOException;
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

    public ServiceProviderImpl(ServiceDiscoveryImpl<T> discovery, String serviceName, ProviderStrategy<T> providerStrategy, ThreadFactory threadFactory, final DownInstanceManager downInstanceManager)
    {
        this.discovery = discovery;
        this.providerStrategy = providerStrategy;
        cache = discovery.serviceCacheBuilder().name(serviceName).threadFactory(threadFactory).build();

        instanceProvider = new InstanceProvider<T>()
        {
            @Override
            public List<ServiceInstance<T>> getInstances() throws Exception
            {
                List<ServiceInstance<T>> instances = cache.getInstances();
                if ( (downInstanceManager != null) && downInstanceManager.hasEntries() )
                {
                    Iterable<ServiceInstance<T>> filtered = Iterables.filter
                    (
                        instances,
                        new Predicate<ServiceInstance<T>>()
                        {
                            @Override
                            public boolean apply(ServiceInstance<T> instance)
                            {
                                return !downInstanceManager.contains(instance);
                            }
                        }
                    );
                    instances = ImmutableList.copyOf(filtered);
                }
                return instances;
            }
        };
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
        return providerStrategy.getInstance(instanceProvider);
    }
}
