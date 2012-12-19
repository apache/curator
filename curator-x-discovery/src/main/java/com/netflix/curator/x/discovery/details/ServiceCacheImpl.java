/*
 * Copyright 2012 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package com.netflix.curator.x.discovery.details;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.listen.ListenerContainer;
import com.netflix.curator.framework.recipes.cache.ChildData;
import com.netflix.curator.framework.recipes.cache.PathChildrenCache;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheEvent;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheListener;
import com.netflix.curator.utils.ZKPaths;
import com.netflix.curator.x.discovery.ServiceCache;
import com.netflix.curator.x.discovery.ServiceInstance;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;

public class ServiceCacheImpl<T> implements ServiceCache<T>, PathChildrenCacheListener
{
    private final ListenerContainer<ServiceCacheListener>           listenerContainer = new ListenerContainer<ServiceCacheListener>();
    private final ServiceDiscoveryImpl<T>                           discovery;
    private final AtomicReference<State>                            state = new AtomicReference<State>(State.LATENT);
    private final PathChildrenCache                                 cache;
    private final Map<String, ServiceInstance<T>>                   instances = Maps.newConcurrentMap();

    private enum State
    {
        LATENT,
        STARTED,
        STOPPED
    }

    ServiceCacheImpl(ServiceDiscoveryImpl<T> discovery, String name, ThreadFactory threadFactory)
    {
        Preconditions.checkNotNull(threadFactory, "threadFactory cannot be null");

        this.discovery = discovery;

        cache = new PathChildrenCache(discovery.getClient(), discovery.pathForName(name), true, threadFactory);
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

        synchronized(this)
        {
            // must be synchronized as PathChildrenCache might be posting events
            // as we are processing this (now old) data. Note: childEvent() is
            // synchronized as well because of this

            cache.start(true);
            for ( ChildData childData : cache.getCurrentData() )
            {
                addInstance(childData);
            }
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

        Closeables.closeQuietly(cache);

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
    public synchronized void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
    {
        boolean         notifyListeners = false;
        switch ( event.getType() )
        {
            case CHILD_ADDED:
            case CHILD_UPDATED:
            {
                addInstance(event.getData());
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

    private void addInstance(ChildData childData) throws Exception
    {
        String                  instanceId = instanceIdFromData(childData);
        ServiceInstance<T>      serviceInstance = discovery.getSerializer().deserialize(childData.getData());
        instances.put(instanceId, serviceInstance);
        cache.clearDataBytes(childData.getPath());
    }
}
