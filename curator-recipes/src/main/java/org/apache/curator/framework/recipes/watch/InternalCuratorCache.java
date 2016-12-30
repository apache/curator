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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.utils.ThreadUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Exchanger;
import java.util.concurrent.atomic.AtomicInteger;

class InternalCuratorCache extends CuratorCacheBase implements Watcher
{
    private final PersistentWatcher watcher;
    private final CuratorFramework client;
    private final String basePath;
    private final CacheFilter cacheFilter;
    private final RefreshFilter refreshFilter;
    private static final CachedNode nullNode = new CachedNode();
    private static final RefreshFilter nopRefreshFilter = new RefreshFilter()
    {
        @Override
        public boolean descend(String path)
        {
            return false;
        }
    };

    InternalCuratorCache(CuratorFramework client, String path, CacheFilter cacheFilter, final RefreshFilter refreshFilter, Cache<String, CachedNode> cache, boolean sendRefreshEvents, final boolean refreshOnStart)
    {
        super(cache, sendRefreshEvents);
        this.client = Objects.requireNonNull(client, "client cannot be null");
        basePath = Objects.requireNonNull(path, "path cannot be null");
        this.cacheFilter = Objects.requireNonNull(cacheFilter, "cacheFilter cannot be null");
        this.refreshFilter = Objects.requireNonNull(refreshFilter, "primingFilter cannot be null");
        watcher = new PersistentWatcher(client, path)
        {
            @Override
            protected void noteWatcherReset()
            {
                if ( refreshOnStart || (refreshCount() > 0) )
                {
                    internalRefresh(basePath, new Refresher(InternalCuratorCache.this, basePath), refreshFilter);
                }
            }
        };
        watcher.getListenable().addListener(this);
    }

    @Override
    protected void internalStart()
    {
        watcher.start();
    }

    @Override
    protected void internalClose()
    {
        watcher.close();
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
    public CountDownLatch refreshAll()
    {
        return refresh(basePath);
    }

    @Override
    public CountDownLatch refresh(String path)
    {
        Preconditions.checkArgument(path.startsWith(basePath), "Path is not this cache's tree: " + path);

        if ( isStarted() )
        {
            CountDownLatch latch = new CountDownLatch(1);
            Refresher refresher = new Refresher(this, path, latch);
            internalRefresh(path, refresher, refreshFilter);
            return latch;
        }
        return new CountDownLatch(0);
    }

    @VisibleForTesting
    volatile Exchanger<Object> rebuildTestExchanger;

    private void internalRefresh(final String path, final Refresher refresher, final RefreshFilter refreshFilter)
    {
        if ( !isStarted() )
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
                if ( rebuildTestExchanger != null )
                {
                    rebuildTestExchanger.exchange(new Object());
                }
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
                else
                {
                    notifyListeners(CacheEvent.NODE_CHANGED, path);
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
