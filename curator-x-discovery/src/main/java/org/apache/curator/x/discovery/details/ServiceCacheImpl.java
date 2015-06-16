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
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.curator.utils.CloseableExecutorService;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.listen.ListenerContainer;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;
import org.apache.curator.x.discovery.ServiceCache;
import org.apache.curator.x.discovery.ServiceInstance;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;

public class ServiceCacheImpl<T> implements ServiceCache<T>, PathChildrenCacheListener
{
    private final ListenerContainer<ServiceCacheListener>           listenerContainer = new ListenerContainer<ServiceCacheListener>();
    private final ServiceDiscoveryImpl<T>                           discovery;
    private final AtomicReference<State>                            state = new AtomicReference<State>(State.LATENT);
    private final PathChildrenCache                                 cache;
    private final ConcurrentMap<String, ServiceInstance<T>>         instances = Maps.newConcurrentMap();

    private enum State
    {
        LATENT,
        STARTED,
        STOPPED
    }

    private static CloseableExecutorService convertThreadFactory(ThreadFactory threadFactory)
    {
        Preconditions.checkNotNull(threadFactory, "threadFactory cannot be null");
        return new CloseableExecutorService(Executors.newSingleThreadExecutor(threadFactory));
    }

    ServiceCacheImpl(ServiceDiscoveryImpl<T> discovery, String name, ThreadFactory threadFactory)
    {
        this(discovery, name, convertThreadFactory(threadFactory));
    }

    ServiceCacheImpl(ServiceDiscoveryImpl<T> discovery, String name, CloseableExecutorService executorService)
    {
        Preconditions.checkNotNull(discovery, "discovery cannot be null");
        Preconditions.checkNotNull(name, "name cannot be null");
        Preconditions.checkNotNull(executorService, "executorService cannot be null");

        this.discovery = discovery;

        cache = new PathChildrenCache(discovery.getClient(), discovery.pathForName(name), true, false, executorService);
        cache.getListenable().addListener(this);
    }

    @Override
    public List<ServiceInstance<T>> getInstances()
    {
        return Lists.newArrayList(instances.values());
    }

    @Override
    public void start() throws Exception
    {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "Cannot be started more than once");

        cache.start(true);
        for ( ChildData childData : cache.getCurrentData() )
        {
            addInstance(childData, true);
        }
        discovery.cacheOpened(this);
    }

    @Override
    public void close() throws IOException
    {
        Preconditions.checkState(state.compareAndSet(State.STARTED, State.STOPPED), "Already closed or has not been started");

        listenerContainer.forEach
            (
                new Function<ServiceCacheListener, Void>()
                {
                    @Override
                    public Void apply(ServiceCacheListener listener)
                    {
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

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
    {
        boolean         notifyListeners = false;
        switch ( event.getType() )
        {
            case CHILD_ADDED:
            case CHILD_UPDATED:
            {
                addInstance(event.getData(), false);
                notifyListeners = true;
                break;
            }

            case CHILD_REMOVED:
            {
                instances.remove(instanceIdFromData(event.getData()));
                notifyListeners = true;
                break;
            }
        }

        if ( notifyListeners )
        {
            listenerContainer.forEach
            (
                new Function<ServiceCacheListener, Void>()
                {
                    @Override
                    public Void apply(ServiceCacheListener listener)
                    {
                        listener.cacheChanged();
                        return null;
                    }
                }
            );
        }
    }

    private String instanceIdFromData(ChildData childData)
    {
        return ZKPaths.getNodeFromPath(childData.getPath());
    }

    private void addInstance(ChildData childData, boolean onlyIfAbsent) throws Exception
    {
        String                  instanceId = instanceIdFromData(childData);
        ServiceInstance<T>      serviceInstance = discovery.getSerializer().deserialize(childData.getData());
        if ( onlyIfAbsent )
        {
            instances.putIfAbsent(instanceId, serviceInstance);
        }
        else
        {
            instances.put(instanceId, serviceInstance);
        }
        cache.clearDataBytes(childData.getPath(), childData.getStat().getVersion());
    }
}
