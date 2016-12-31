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
import com.google.common.cache.Cache;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.utils.ThreadUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Exchanger;
import java.util.concurrent.atomic.AtomicInteger;

class InternalCuratorCache extends CuratorCacheBase implements Watcher
{
    private final PersistentWatcher watcher;
    private final CuratorFramework client;
    private final String basePath;
    private final CachedNodeComparator nodeComparator;
    private final CacheFilter cacheFilter;
    private final RefreshFilter refreshFilter;
    private final boolean sortChildren;
    private static final CachedNode nullNode = new CachedNode();
    private static final RefreshFilter nopRefreshFilter = new RefreshFilter()
    {
        @Override
        public boolean descend(String mainPath, String checkPath)
        {
            return false;
        }
    };

    InternalCuratorCache(CuratorFramework client, String path, CachedNodeComparator nodeComparator, CacheFilter cacheFilter, final RefreshFilter refreshFilter, Cache<String, CachedNode> cache, boolean sendRefreshEvents, final boolean refreshOnStart, boolean sortChildren)
    {
        super(cache, sendRefreshEvents);
        this.client = Objects.requireNonNull(client, "client cannot be null");
        this.basePath = Objects.requireNonNull(path, "path cannot be null");
        this.nodeComparator = Objects.requireNonNull(nodeComparator, "nodeComparator cannot be null");
        this.cacheFilter = Objects.requireNonNull(cacheFilter, "cacheFilter cannot be null");
        this.refreshFilter = Objects.requireNonNull(refreshFilter, "primingFilter cannot be null");
        this.sortChildren = sortChildren;
        watcher = new PersistentWatcher(client, path)
        {
            @Override
            protected void noteWatcherReset()
            {
                long count = refreshCount();
                if ( (refreshOnStart && (count == 0)) || (count > 0) )
                {
                    internalRefresh(basePath, new NotifyingRefresher(InternalCuratorCache.this, basePath), refreshFilter);
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
                remove(event.getPath());
                break;
            }

            case NodeCreated:
            case NodeDataChanged:
            {
                internalRefresh(event.getPath(), new Refresher(InternalCuratorCache.this), nopRefreshFilter);
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
        if ( isStarted() && path.startsWith(basePath) )
        {
            CountDownLatch latch = new CountDownLatch(1);
            Refresher refresher = new NotifyingRefresher(this, path, latch);
            internalRefresh(path, refresher, refreshFilter);
            return latch;
        }
        return new CountDownLatch(0);
    }

    @VisibleForTesting
    volatile Exchanger<Object> debugRebuildTestExchanger;

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
                        CacheAction cacheAction = (CacheAction)event.getContext();
                        CachedNode newNode = new CachedNode(event.getStat(), event.getData());
                        CachedNode oldNode = putNewNode(path, cacheAction, newNode);
                        if ( oldNode == null )
                        {
                            notifyListeners(CacheEvent.NODE_CREATED, path, newNode);
                        }
                        else if ( !nodeComparator.isSame(newNode, oldNode) )
                        {
                            notifyListeners(CacheEvent.NODE_CHANGED, path, newNode);
                        }
                    }
                    else if ( event.getType() == CuratorEventType.CHILDREN )
                    {
                        List<String> children = event.getChildren();
                        checkDeletedChildren(path, children);
                        if ( sortChildren )
                        {
                            Collections.sort(children);
                        }
                        for ( String child : children )
                        {
                            internalRefresh(ZKPaths.makePath(path, child), refresher, refreshFilter);
                        }
                    }
                }
                else if ( (event.getType() == CuratorEventType.CHILDREN) && (event.getResultCode() == KeeperException.Code.NONODE.intValue()) )
                {
                    checkDeletedChildren(path, Collections.<String>emptyList());
                }
                else
                {
                    // TODO
                }
                refresher.decrement();
                if ( debugRebuildTestExchanger != null )
                {
                    debugRebuildTestExchanger.exchange(new Object());
                }
            }
        };

        CacheAction cacheAction = cacheFilter.actionForPath(basePath, path);
        switch ( cacheAction )
        {
            case NOT_STORED:
            {
                // NOP
                break;
            }

            case STAT_ONLY:
            case STAT_AND_DATA:
            {
                try
                {
                    refresher.increment();
                    client.getData().inBackground(callback, cacheAction).forPath(path);
                }
                catch ( Exception e )
                {
                    ThreadUtils.checkInterrupted(e);
                    // TODO
                }
                break;
            }

            case STAT_AND_COMPRESSED_DATA:
            {
                try
                {
                    refresher.increment();
                    client.getData().decompressed().inBackground(callback, cacheAction).forPath(path);
                }
                catch ( Exception e )
                {
                    ThreadUtils.checkInterrupted(e);
                    // TODO
                }
                break;
            }
        }

        if ( refreshFilter.descend(basePath, path) )
        {
            refresher.increment();
            try
            {
                client.getChildren().inBackground(callback, cacheAction).forPath(path);
            }
            catch ( Exception e )
            {
                ThreadUtils.checkInterrupted(e);
                // TODO
            }
        }
    }

    private void checkDeletedChildren(String path, List<String> children)
    {
        Collection<String> namesAtPath = childNamesAtPath(path);
        Sets.SetView<String> deleted = Sets.difference(Sets.newHashSet(namesAtPath), Sets.newHashSet(children));
        for ( String deletedName : deleted )
        {
            remove(ZKPaths.makePath(path, deletedName));
        }
    }

    private CachedNode putNewNode(String path, CacheAction cacheAction, CachedNode newNode)
    {
        CachedNode putNode;
        switch ( cacheAction )
        {
            default:
            case NOT_STORED:
            {
                throw new IllegalStateException(String.format("Should not be here with action %s for path %s", cacheAction, path));
            }

            case PATH_ONLY:
            {
                putNode = nullNode;
                break;
            }

            case STAT_ONLY:
            {
                putNode = new CachedNode(newNode.getStat());
                break;
            }

            case STAT_AND_DATA:
            case STAT_AND_COMPRESSED_DATA:
            {
                putNode = newNode;
                break;
            }
        }
        return cache.asMap().put(path, putNode);
    }

    private void decrementOutstanding(SettableFuture<Boolean> task, AtomicInteger outstandingCount)
    {
        if ( outstandingCount.decrementAndGet() <= 0 )
        {
            task.set(true);
        }
    }

    private void remove(String path)
    {
        CachedNode removed = cache.asMap().remove(path);
        if ( removed != null )
        {
            notifyListeners(CacheEvent.NODE_DELETED, path, removed);
        }
    }
}
