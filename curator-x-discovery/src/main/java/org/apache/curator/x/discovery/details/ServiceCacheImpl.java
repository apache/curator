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

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.framework.listen.ListenerContainer;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.x.discovery.ServiceCache;
import org.apache.curator.x.discovery.ServiceInstance;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;

public class ServiceCacheImpl<T> extends AbstractNodeCache<T> implements ServiceCache<T>
{
    private final ListenerContainer<ServiceCacheListener>           listenerContainer = new ListenerContainer<ServiceCacheListener>();
    private final AtomicReference<State>                            state = new AtomicReference<State>(State.LATENT);
    private final String                                            serviceName;

    private Optional<ServiceInstancesCache<T>> serviceInstancesCache = Optional.absent();

    private enum State
    {
        LATENT,
        STARTED,
        STOPPED
    }

    ServiceCacheImpl(ServiceDiscoveryImpl<T> discovery, String basePath, String name, ThreadFactory threadFactory)
    {
        super(discovery, basePath, threadFactory);
        serviceName = name;
    }

    @Override
    void onChildAdded(PathChildrenCacheEvent event) throws Exception
    {
        if (isEventRelatedWithService(event))
        {
            ServiceInstancesCache<T> instance = new ServiceInstancesCache<T>(discovery, serviceName, threadFactory, this);
            instance.start();
            serviceInstancesCache = Optional.of(instance);
        }

    }

    @Override
    void onChildUpdated(PathChildrenCacheEvent event) {

    }

    @Override
    void onChildRemoved(PathChildrenCacheEvent event) throws Exception
    {
        if (isEventRelatedWithService(event))
        {
            serviceInstancesCache.get().close();
            serviceInstancesCache = Optional.absent();
        }
    }

    private boolean isEventRelatedWithService(PathChildrenCacheEvent event) {
        return event.getData().getPath().endsWith(String.format("/%s",serviceName));
    }

    @Override
    public List<ServiceInstance<T>> getInstances()
    {
        return serviceInstancesCache.isPresent() ? Lists.newArrayList(serviceInstancesCache.get().getInstances()) : Collections.EMPTY_LIST;
    }

    @Override
    public void start() throws Exception
    {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "Cannot be started more than once");

        super.start();

        discovery.cacheOpened(this);
    }

    @Override
    public void close() throws IOException
    {
        Preconditions.checkState(state.compareAndSet(State.STARTED, State.STOPPED), "Already closed or has not been started");

        listenerContainer.forEach
                (
                        new Function<ServiceCacheListener, Void>() {
                            @Override
                            public Void apply(ServiceCacheListener listener) {
                                discovery.getClient().getConnectionStateListenable().removeListener(listener);
                                return null;
                            }
                        }
                );
        listenerContainer.clear();

        CloseableUtils.closeQuietly(cache);

        discovery.cacheClosed(this);
    }

    @Override
    public void addListener(ServiceCacheListener listener)
    {
        listenerContainer.addListener(listener);
        discovery.getClient().getConnectionStateListenable().addListener(listener);
    }

    @Override
    public void addListener(ServiceCacheListener listener, Executor executor)
    {
        listenerContainer.addListener(listener, executor);
        discovery.getClient().getConnectionStateListenable().addListener(listener, executor);
    }

    @Override
    public void removeListener(ServiceCacheListener listener)
    {
        listenerContainer.removeListener(listener);
        discovery.getClient().getConnectionStateListenable().removeListener(listener);
    }

    void forEach(Function<ServiceCacheListener, Void> function)
    {
        listenerContainer.forEach(function);
    }
}

