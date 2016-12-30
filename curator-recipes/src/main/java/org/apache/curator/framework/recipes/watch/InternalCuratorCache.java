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

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.utils.ThreadUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

class InternalCuratorCache extends CuratorCacheBase implements Watcher
{
    private final PersistentWatcher watcher;
    private final CuratorFramework client;
    private final String basePath;
    private final CacheFilter cacheFilter;
    private final RefreshFilter refreshFilter;
    private final boolean refreshOnStart;
    private static final CachedNode nullNode = new CachedNode();
    private static final RefreshFilter nopRefreshFilter = new RefreshFilter()
    {
        @Override
        public boolean descend(String path)
        {
            return false;
        }
    };
    private final ConnectionStateListener connectionStateListener = new ConnectionStateListener()
    {
        @Override
        public void stateChanged(CuratorFramework client, ConnectionState newState)
        {
            if ( newState.isConnected() )
            {
                internalRefresh(basePath, new Refresher(InternalCuratorCache.this, basePath), refreshFilter);
            }
        }
    };

    InternalCuratorCache(CuratorFramework client, String path, CacheFilter cacheFilter, RefreshFilter refreshFilter, Cache<String, CachedNode> cache, boolean sendRefreshEvents, boolean refreshOnStart)
    {
        super(cache, sendRefreshEvents);
        this.client = Objects.requireNonNull(client, "client cannot be null");
        basePath = Objects.requireNonNull(path, "path cannot be null");
        this.cacheFilter = Objects.requireNonNull(cacheFilter, "cacheFilter cannot be null");
        this.refreshFilter = Objects.requireNonNull(refreshFilter, "primingFilter cannot be null");
        this.refreshOnStart = refreshOnStart;
        watcher = new PersistentWatcher(client, path);
        watcher.getListenable().addListener(this);
    }

    @Override
    public void start()
    {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "already started");
        watcher.start();
        client.getConnectionStateListenable().addListener(connectionStateListener);
        if ( refreshOnStart )
        {
            internalRefresh(basePath, new Refresher(InternalCuratorCache.this, basePath), refreshFilter);
        }
    }

    @Override
    public void close()
    {
        if ( state.compareAndSet(State.STARTED, State.CLOSED) )
        {
            client.getConnectionStateListenable().removeListener(connectionStateListener);
            watcher.getListenable().removeListener(this);
            listeners.clear();
            watcher.close();
        }
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
                    notifyListeners(CacheEvent.NODE_DELETED, event.getPath());
                }
                break;
            }

            case NodeCreated:
            case NodeDataChanged:
            {
                internalRefresh(event.getPath(), new Refresher(InternalCuratorCache.this, basePath), nopRefreshFilter);
                break;
            }
        }
    }

    @Override
    public Future<Boolean> refreshAll()
    {
        return refresh(basePath);
    }

    @Override
    public Future<Boolean> refresh(String path)
    {
        Preconditions.checkArgument(path.startsWith(basePath), "Path is not this cache's tree: " + path);

        if ( state.get() == State.STARTED )
        {
            SettableFuture<Boolean> task = SettableFuture.create();
            Refresher refresher = new Refresher(this, path, task);
            internalRefresh(path, refresher, refreshFilter);
            return task;
        }
        return Futures.immediateFuture(true);
    }

    private void internalRefresh(final String path, final Refresher refresher, final RefreshFilter refreshFilter)
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
                if ( event.getResultCode() == 0 )
                {
                    if ( event.getType() == CuratorEventType.GET_DATA )
                    {
                        CachedNode newNode = new CachedNode(event.getStat(), event.getData());
                        CachedNode oldNode = cache.asMap().put(path, newNode);
                        if ( oldNode == null )
                        {
                            notifyListeners(CacheEvent.NODE_CREATED, path);
                        }
                        else if ( !newNode.equals(oldNode) )
                        {
                            notifyListeners(CacheEvent.NODE_CHANGED, path);
                        }
                    }
                    else if ( event.getType() == CuratorEventType.CHILDREN )
                    {
                        for ( String child : event.getChildren() )
                        {
                            internalRefresh(ZKPaths.makePath(path, child), refresher, refreshFilter);
                        }
                    }
                }
                else
                {
                    // TODO
                }
                refresher.decrement();
            }
        };

        switch ( cacheFilter.actionForPath(path) )
        {
            case NOT_STORED:
            {
                // NOP
                break;
            }

            case PATH_ONLY:
            {
                if ( cache.asMap().put(path, nullNode) == null )
                {
                    notifyListeners(CacheEvent.NODE_CREATED, path);
                }
                break;
            }

            case PATH_AND_DATA:
            {
                try
                {
                    refresher.increment();
                    client.getData().inBackground(callback).forPath(path);
                }
                catch ( Exception e )
                {
                    ThreadUtils.checkInterrupted(e);
                    // TODO
                }
                break;
            }

            case PATH_AND_COMPRESSED_DATA:
            {
                try
                {
                    refresher.increment();
                    client.getData().decompressed().inBackground(callback).forPath(path);
                }
                catch ( Exception e )
                {
                    ThreadUtils.checkInterrupted(e);
                    // TODO
                }
                break;
            }
        }

        if ( refreshFilter.descend(path) )
        {
            refresher.increment();
            try
            {
                client.getChildren().inBackground(callback).forPath(path);
            }
            catch ( Exception e )
            {
                ThreadUtils.checkInterrupted(e);
                // TODO
            }
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
