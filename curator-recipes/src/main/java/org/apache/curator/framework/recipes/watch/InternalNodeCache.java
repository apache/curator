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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Exchanger;
import java.util.concurrent.atomic.AtomicBoolean;

class InternalNodeCache extends CuratorCacheBase
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final WatcherRemoveCuratorFramework client;
    private final String path;
    private final CacheAction cacheAction;
    private final CachedNodeComparator nodeComparator;
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
                log.debug(String.format("Could not processBackgroundResult(%s). Should refresh when reconnected", event), e);
            }
        }
    };

    // TODO refreshOnStart
    InternalNodeCache(CuratorFramework client, String path, CacheAction cacheAction, CachedNodeComparator nodeComparator, CachedNodeMap cache, boolean sendRefreshEvents, boolean refreshOnStart)
    {
        super(path, cache, sendRefreshEvents);
        this.client = client.newWatcherRemoveCuratorFramework();
        this.path = PathUtils.validatePath(path);
        this.cacheAction = cacheAction;
        this.nodeComparator = nodeComparator;
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
        return refresh(path);
    }

    @Override
    public CountDownLatch refresh(String path)
    {
        Preconditions.checkArgument(this.path.equals(path), "Bad path: " + path);

        CountDownLatch latch = new CountDownLatch(1);
        NotifyingRefresher refresher = new NotifyingRefresher(this, path, latch);
        try
        {
            reset(refresher);
        }
        catch ( Exception e )
        {
            ThreadUtils.checkInterrupted(e);
            log.debug(String.format("Could not refresh for path: %s - should refresh next reconnect", path), e);
        }
        return latch;
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
                    switch ( cacheAction )
                    {
                        default:
                        case NOT_STORED:
                        {
                            throw new UnsupportedOperationException("Single node cache does not support action: IGNORE");
                        }

                        case STAT_ONLY:
                        {
                            setNewData(nullNode);
                            break;
                        }

                        case STAT_AND_DATA:
                        {
                            refresher.increment();
                            client.getData().usingWatcher(watcher).inBackground(backgroundCallback, refresher).forPath(path);
                            break;
                        }

                        case STAT_AND_UNCOMPRESSED_DATA:
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
        CachedNode previousData = (newData != null) ? cache.put(path, newData) : cache.remove(path);
        if ( newData == null )
        {
            if ( previousData != null )
            {
                notifyListeners(CacheEvent.NODE_DELETED, path, newData);
            }
        }
        else if ( previousData == null )
        {
            notifyListeners(CacheEvent.NODE_CREATED, path, newData);
        }
        else if ( !nodeComparator.isSame(previousData, newData) )
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
