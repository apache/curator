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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.EnsureContainers;
import org.apache.curator.framework.listen.StandardListenerManager;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheBridge;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.curator.x.discovery.ServiceCache;
import org.apache.curator.x.discovery.ServiceInstance;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;

public class ServiceCacheImpl<T> implements ServiceCache<T>, PathChildrenCacheListener
{
    private final StandardListenerManager<ServiceCacheListener> listenerContainer = StandardListenerManager.standard();
    private final ServiceDiscoveryImpl<T> discovery;
    private final AtomicReference<State> state = new AtomicReference<>(State.LATENT);
    private final CuratorCacheBridge cache;
    private final ConcurrentMap<String, ServiceInstance<T>> instances = Maps.newConcurrentMap();
    private final EnsureContainers ensureContainers;
    private final CountDownLatch initializedLatch = new CountDownLatch(1);

    private enum State
    {
        LATENT, STARTED, STOPPED
    }

    private static ExecutorService convertThreadFactory(ThreadFactory threadFactory)
    {
        Preconditions.checkNotNull(threadFactory, "threadFactory cannot be null");
        return Executors.newSingleThreadExecutor(threadFactory);
    }

    ServiceCacheImpl(ServiceDiscoveryImpl<T> discovery, String name, ThreadFactory threadFactory)
    {
        this(discovery, name, convertThreadFactory(threadFactory));
    }

    ServiceCacheImpl(ServiceDiscoveryImpl<T> discovery, String name, ExecutorService executorService)
    {
        Preconditions.checkNotNull(discovery, "discovery cannot be null");
        Preconditions.checkNotNull(name, "name cannot be null");

        this.discovery = discovery;

        String path = discovery.pathForName(name);
        cache = CuratorCache.bridgeBuilder(discovery.getClient(), path)
            .withExecutorService(executorService)
            .withDataNotCached()
            .build();
        CuratorCacheListener listener = CuratorCacheListener.builder()
            .forPathChildrenCache(path, discovery.getClient(), this)
            .forInitialized(this::initialized)
            .build();
        cache.listenable().addListener(listener);
        ensureContainers = new EnsureContainers(discovery.getClient(), path);
    }

    @Override
    public List<ServiceInstance<T>> getInstances()
    {
        return Lists.newArrayList(instances.values());
    }

    @VisibleForTesting
    volatile CountDownLatch debugStartLatch = null;
    volatile CountDownLatch debugStartWaitLatch = null;

    @Override
    public void start() throws Exception
    {
        startImmediate().await();
    }

    @Override
    public CountDownLatch startImmediate() throws Exception
    {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "Cannot be started more than once");

        ensureContainers.ensure();
        cache.start();
        if ( debugStartLatch != null )
        {
            initializedLatch.await();
            debugStartLatch.countDown();
            debugStartLatch = null;
        }
        if ( debugStartWaitLatch != null )
        {
            debugStartWaitLatch.await();
            debugStartWaitLatch = null;
        }

        return initializedLatch;
    }

    @Override
    public void close()
    {
        Preconditions.checkState(state.compareAndSet(State.STARTED, State.STOPPED), "Already closed or has not been started");

        listenerContainer.forEach(l -> discovery.getClient().getConnectionStateListenable().removeListener(l));
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
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)
    {
        boolean notifyListeners = false;
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

        if ( notifyListeners && (initializedLatch.getCount() == 0) )
        {
            listenerContainer.forEach(ServiceCacheListener::cacheChanged);
        }
    }

    private String instanceIdFromData(ChildData childData)
    {
        return ZKPaths.getNodeFromPath(childData.getPath());
    }

    private void addInstance(ChildData childData)
    {
        try
        {
            String instanceId = instanceIdFromData(childData);
            ServiceInstance<T> serviceInstance = discovery.getSerializer().deserialize(childData.getData());
            instances.put(instanceId, serviceInstance);
        }
        catch ( Exception e )
        {
            throw new RuntimeException(e);
        }
    }

    private void initialized()
    {
        discovery.cacheOpened(this);
        initializedLatch.countDown();
    }
}
