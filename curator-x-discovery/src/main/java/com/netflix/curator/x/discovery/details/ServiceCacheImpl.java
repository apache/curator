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
import com.google.common.collect.ImmutableList;
import com.netflix.curator.framework.listen.ListenerContainer;
import com.netflix.curator.x.discovery.ServiceInstance;
import com.netflix.curator.x.discovery.ServiceCache;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;

public class ServiceCacheImpl<T> implements ServiceCache<T>
{
    private final Logger                                            log = LoggerFactory.getLogger(getClass());
    private final ListenerContainer<ServiceCacheListener>           listenerContainer = new ListenerContainer<ServiceCacheListener>();
    private final ServiceDiscoveryImpl<T> discovery;
    private final String                                            name;
    private final int                                               refreshPaddingMs;
    private final ExecutorService                                   executorService;
    private final Latch refreshLatch = new Latch();
    private final AtomicReference<State>                            state = new AtomicReference<State>(State.LATENT);
    private final AtomicReference<List<ServiceInstance<T>>>         instances = new AtomicReference<List<ServiceInstance<T>>>(ImmutableList.<ServiceInstance<T>>of());
    private final Watcher                                           watcher = new Watcher()
    {
        @Override
        public void process(WatchedEvent event)
        {
            refreshLatch.set();
        }
    };

    private enum State
    {
        LATENT,
        STARTED,
        STOPPED
    }

    ServiceCacheImpl(ServiceDiscoveryImpl<T> discovery, String name, ThreadFactory threadFactory, int refreshPaddingMs)
    {
        Preconditions.checkNotNull(threadFactory, "threadFactory cannot be null");
        Preconditions.checkArgument(refreshPaddingMs >= 0, "refreshPaddingMs cannot be negative");

        this.discovery = discovery;
        this.name = name;
        this.refreshPaddingMs = refreshPaddingMs;

        executorService = Executors.newSingleThreadExecutor(threadFactory);
    }

    @Override public List<ServiceInstance<T>> getInstances()
    {
        return instances.get();
    }

    @Override public void start() throws Exception
    {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "already started");

        executorService.submit
        (
            new Callable<Void>()
            {
                @Override
                public Void call() throws Exception
                {
                    doWork();
                    return null;
                }
            }
        );

        refresh(false);
        
        discovery.cacheOpened(this);
    }

    @Override
    public void close() throws IOException
    {
        Preconditions.checkState(state.compareAndSet(State.STARTED, State.STOPPED), "not started");

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

        // Interrupt our own worker thread and shutdown thread pool
        executorService.shutdownNow();

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

    private void doWork()
    {
        while ( !Thread.currentThread().isInterrupted() )
        {
            try
            {
                if ( refreshPaddingMs > 0 )
                {
                    Thread.sleep(refreshPaddingMs);
                }
                
                refreshLatch.await();
                refresh(true);
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void refresh(boolean notifyListeners)
    {
        try
        {
            List<ServiceInstance<T>> theInstances = discovery.queryForInstances(name, watcher);
            instances.set(theInstances);

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
        catch ( Exception e )
        {
            log.error("ServiceCache.refresh()", e);
        }
    }
}
