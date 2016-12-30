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
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.WatcherRemoveCuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.utils.PathUtils;
import org.apache.curator.utils.ThreadUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Exchanger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

class InternalNodeCache extends CuratorCacheBase
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final WatcherRemoveCuratorFramework client;
    private final String path;
    private final CacheFilter cacheFilter;
    private final AtomicReference<CachedNode> data = new AtomicReference<>(null);
    private final AtomicBoolean isConnected = new AtomicBoolean(true);
    private static final CachedNode nullNode = new CachedNode();
    private final ConnectionStateListener connectionStateListener = new ConnectionStateListener()
    {
        @Override
        public void stateChanged(CuratorFramework client, ConnectionState newState)
        {
            if ( (newState == ConnectionState.CONNECTED) || (newState == ConnectionState.RECONNECTED) )
            {
                if ( isConnected.compareAndSet(false, true) )
                {
                    refreshAll();
                }
            }
            else
            {
                isConnected.set(false);
            }
        }
    };

    private Watcher watcher = new Watcher()
    {
        @Override
        public void process(WatchedEvent event)
        {
            refreshAll();
        }
    };

    private final BackgroundCallback backgroundCallback = new BackgroundCallback()
    {
        @Override
        public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
        {
            try
            {
                processBackgroundResult(event);
            }
            catch ( Exception e )
            {
                ThreadUtils.checkInterrupted(e);
                // TODO
            }
        }
    };

    InternalNodeCache(CuratorFramework client, String path, CacheFilter cacheFilter, Cache<String, CachedNode> cache, boolean sendRefreshEvents, boolean refreshOnStart)
    {
        super(cache, sendRefreshEvents);
        this.client = client.newWatcherRemoveCuratorFramework();
        this.path = PathUtils.validatePath(path);
        this.cacheFilter = Objects.requireNonNull(cacheFilter, "cacheFilter cannot be null");
    }

    @Override
    protected void internalStart()
    {
        client.getConnectionStateListenable().addListener(connectionStateListener);
        refreshAll();
    }

    @Override
    protected void internalClose()
    {
        client.removeWatchers();
        client.getConnectionStateListenable().removeListener(connectionStateListener);
    }

    @Override
    public CountDownLatch refreshAll()
    {
        return null;    // TODO
    }

    @Override
    public CountDownLatch refresh(String path)
    {
        return null;    // TODO
    }

    public void refreXshAll()
    {
        try
        {
            reset(null);
        }
        catch ( Exception e )
        {
            ThreadUtils.checkInterrupted(e);
            // TODO
        }
    }

    public void reXfresh(String path)
    {
        Preconditions.checkArgument(this.path.equals(path), "Bad path: " + path);
        refreshAll();
    }

    private void reset(Refresher refresher) throws Exception
    {
        if ( isStarted() && isConnected.get() )
        {
            refresher.increment();
            client.checkExists().usingWatcher(watcher).inBackground(backgroundCallback, refresher).forPath(path);
        }
    }

    private void processBackgroundResult(CuratorEvent event) throws Exception
    {
        Refresher refresher = (Refresher)event.getContext();
        switch ( event.getType() )
        {
            case GET_DATA:
            {
                if ( event.getResultCode() == KeeperException.Code.OK.intValue() )
                {
                    CachedNode cachedNode = new CachedNode(event.getStat(), event.getData());
                    setNewData(cachedNode);
                }
                break;
            }

            case EXISTS:
            {
                if ( event.getResultCode() == KeeperException.Code.NONODE.intValue() )
                {
                    setNewData(null);
                }
                else if ( event.getResultCode() == KeeperException.Code.OK.intValue() )
                {
                    switch ( cacheFilter.actionForPath(path) )
                    {
                        default:
                        case NOT_STORED:
                        {
                            throw new UnsupportedOperationException("Single node cache does not support action: IGNORE");
                        }

                        case PATH_ONLY:
                        {
                            setNewData(nullNode);
                            break;
                        }

                        case PATH_AND_DATA:
                        {
                            refresher.increment();
                            client.getData().usingWatcher(watcher).inBackground(backgroundCallback, refresher).forPath(path);
                            break;
                        }

                        case PATH_AND_COMPRESSED_DATA:
                        {
                            refresher.increment();
                            client.getData().decompressed().usingWatcher(watcher).inBackground(backgroundCallback, refresher).forPath(path);
                            break;
                        }
                    }
                }
                break;
            }
        }

        refresher.decrement();
    }

    @VisibleForTesting
    volatile Exchanger<Object> rebuildTestExchanger;
    private void setNewData(CachedNode newData) throws InterruptedException
    {
        CachedNode previousData = data.getAndSet(newData);
        if ( newData == null )
        {
            notifyListeners(CacheEvent.NODE_DELETED, path, newData);
        }
        else if ( previousData == null )
        {
            notifyListeners(CacheEvent.NODE_CREATED, path, newData);
        }
        else if ( !previousData.equals(newData) )
        {
            notifyListeners(CacheEvent.NODE_CHANGED, path, newData);
        }

        if ( rebuildTestExchanger != null )
        {
            try
            {
                rebuildTestExchanger.exchange(new Object());
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
            }
        }
    }
}
