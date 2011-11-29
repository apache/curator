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

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.netflix.curator.framework.listen.Listenable;
import com.netflix.curator.framework.listen.ListenerContainer;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Maintains a cache of instances for a given named service
 */
public class ServiceCache<T> implements Closeable, Listenable<ServiceCacheListener>
{
    private final ListenerContainer<ServiceCacheListener>           listenerContainer = new ListenerContainer<ServiceCacheListener>();
    private final ServiceDiscovery<T>                               discovery;
    private final String                                            name;
    private final AtomicReference<Exception>                        lastException = new AtomicReference<Exception>(null);
    private final AtomicReference<Collection<ServiceInstance<T>>>   instances = new AtomicReference<Collection<ServiceInstance<T>>>(ImmutableList.<ServiceInstance<T>>of());
    private final Watcher                                           watcher = new Watcher()
    {
        @Override
        public void process(WatchedEvent event)
        {
            refresh(true);
        }
    };

    ServiceCache(ServiceDiscovery<T> discovery, String name)
    {
        this.discovery = discovery;
        this.name = name;
    }

    /**
     * Return the current list of instances. NOTE: there is no guarantee of freshness. This is
     * merely the last known list of instances. However, the list is updated via a ZooKeeper watcher
     * so it should be fresh within a window of a second or two.
     *
     * @return the list
     * @throws Exception errors
     */
    public Collection<ServiceInstance<T>>       getInstances() throws Exception
    {
        checkLastException();
        return instances.get();
    }

    /**
     * The cache must be started before use
     *
     * @throws Exception errors
     */
    public void start() throws Exception
    {
        refresh(false);
        checkLastException();
    }

    @Override
    public void close() throws IOException
    {
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

    private void refresh(boolean notifyListeners)
    {
        try
        {
            Collection<ServiceInstance<T>> theInstances = discovery.queryForInstances(name, watcher);
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
            discovery.getClient().getZookeeperClient().getLog().error("ServiceCache.refresh()", e);
            lastException.set(e);
        }
    }

    private void checkLastException() throws Exception
    {
        Exception exception = lastException.getAndSet(null);
        if ( exception != null )
        {
            throw exception;
        }
    }
}
