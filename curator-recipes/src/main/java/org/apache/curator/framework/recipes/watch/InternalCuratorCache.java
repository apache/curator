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
package org.apache.curator.framework.recipes.watch;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.listen.ListenerContainer;
import org.apache.curator.utils.ThreadUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

class InternalCuratorCache extends CuratorCacheBase implements Watcher
{
    private final AtomicReference<State> state = new AtomicReference<>(State.LATENT);
    private final PersistentWatcher watcher;
    private final CuratorFramework client;
    private final String basePath;
    private final CacheFilter cacheFilter;
    private final PrimingFilter primingFilter;
    private final ListenerContainer<CacheListener> listeners = new ListenerContainer<>();
    private static final CachedNode nullNode = new CachedNode();

    private enum State
    {
        LATENT,
        STARTED,
        CLOSED
    }

    InternalCuratorCache(CuratorFramework client, String path, CacheFilter cacheFilter, PrimingFilter primingFilter, Cache<String, CachedNode> cache)
    {
        super(cache);
        this.client = Objects.requireNonNull(client, "client cannot be null");
        basePath = Objects.requireNonNull(path, "path cannot be null");
        this.cacheFilter = Objects.requireNonNull(cacheFilter, "cacheFilter cannot be null");
        this.primingFilter = Objects.requireNonNull(primingFilter, "primingFilter cannot be null");
        watcher = new PersistentWatcher(client, path)
        {
            @Override
            protected void watcherSet()
            {
                refreshAll();
            }
        };
        watcher.getListenable().addListener(this);
    }

    @Override
    public void start()
    {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "already started");
        watcher.start();
    }

    @Override
    public void close()
    {
        if ( state.compareAndSet(State.STARTED, State.CLOSED) )
        {
            watcher.getListenable().removeListener(this);
            listeners.clear();
            watcher.close();
        }
    }

    @Override
    public Listenable<CacheListener> getListenable()
    {
        return listeners;
    }

    @Override
    public Future<Boolean> primeCache()
    {
        Preconditions.checkState(state.get() == State.STARTED, "not started");
        Primer primer = new Primer();
        internalPrimeCache(basePath, primer);
        return primer.getTask();
    }

    @Override
    public void process(WatchedEvent event)
    {
        switch ( event.getType() )
        {
            default:
            {
                // NOP
                break;
            }

            case NodeDeleted:
            {
                if ( cache.asMap().remove(event.getPath()) != null )
                {
                    notifyListeners(CacheEventType.NODE_DELETED, event.getPath());
                }
                break;
            }

            case NodeCreated:
            case NodeDataChanged:
            {
                refresh(event.getPath());
                break;
            }
        }
    }

    @Override
    public void refreshAll()
    {
        Set<String> keySet = new HashSet<>(cache.asMap().keySet());
        for ( String path : keySet )
        {
            refresh(path);
        }
    }

    @Override
    public void refresh(final String path)
    {
        internalRefresh(path, null);
    }

    private void internalRefresh(final String path, final Primer primer)
    {
        if ( state.get() != State.STARTED )
        {
            return;
        }

        BackgroundCallback callback = new BackgroundCallback()
        {
            @Override
            public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
            {
                if ( event.getType() == CuratorEventType.GET_DATA )
                {
                    CachedNode newNode = new CachedNode(event.getStat(), event.getData());
                    CachedNode oldNode = cache.asMap().put(path, newNode);
                    if ( oldNode == null )
                    {
                        notifyListeners(CacheEventType.NODE_CREATED, path);
                    }
                    else if ( !newNode.equals(oldNode) )
                    {
                        notifyListeners(CacheEventType.NODE_CHANGED, path);
                    }
                }
                if ( primer != null )
                {
                    primer.decrement();
                }
            }
        };

        switch ( cacheFilter.actionForPath(path) )
        {
            case IGNORE:
            {
                // NOP
                break;
            }

            case DO_NOT_GET_DATA:
            {
                if ( cache.asMap().put(path, nullNode) == null )
                {
                    notifyListeners(CacheEventType.NODE_CREATED, path);
                }
                break;
            }

            case GET_DATA:
            {
                try
                {
                    if ( primer != null )
                    {
                        primer.increment();
                    }
                    client.getData().inBackground(callback).forPath(path);
                }
                catch ( Exception e )
                {
                    ThreadUtils.checkInterrupted(e);
                    // TODO
                }
                break;
            }

            case GET_COMPRESSED:
            {
                try
                {
                    if ( primer != null )
                    {
                        primer.increment();
                    }
                    client.getData().decompressed().inBackground().forPath(path);
                }
                catch ( Exception e )
                {
                    ThreadUtils.checkInterrupted(e);
                    // TODO
                }
                break;
            }
        }
    }

    private void notifyListeners(final CacheEventType eventType, final String path)
    {
        if ( state.get() != State.STARTED )
        {
            return;
        }

        Function<CacheListener, Void> proc = new Function<CacheListener, Void>()
        {
            @Override
            public Void apply(CacheListener listener)
            {
                listener.process(eventType, path);
                return null;
            }
        };
        listeners.forEach(proc);
    }

    private void internalPrimeCache(final String path, final Primer primer)
    {
        if ( (state.get() != State.STARTED) || primer.getTask().isCancelled() )
        {
            return;
        }

        BackgroundCallback callback = new BackgroundCallback()
        {
            @Override
            public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
            {
                if ( event.getType() == CuratorEventType.CHILDREN )
                {
                    for ( String child : event.getChildren() )
                    {
                        String refreshPath = ZKPaths.makePath(path, child);
                        refresh(refreshPath);
                        if ( primingFilter.descend(refreshPath) )
                        {
                            internalPrimeCache(refreshPath, primer);
                        }
                    }
                }
                primer.decrement();
            }
        };
        try
        {
            primer.increment();
            client.getChildren().inBackground(callback).forPath(path);
        }
        catch ( Exception e )
        {
            ThreadUtils.checkInterrupted(e);
            // TODO
        }
    }

    private void decrementOutstanding(SettableFuture<Boolean> task, AtomicInteger outstandingCount)
    {
        if ( outstandingCount.decrementAndGet() <= 0 )
        {
            task.set(true);
        }
    }
}
