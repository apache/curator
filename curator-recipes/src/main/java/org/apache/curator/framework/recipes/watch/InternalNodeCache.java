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
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.WatcherRemoveCuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.listen.ListenerContainer;
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
    private final AtomicReference<State> state = new AtomicReference<>(State.LATENT);
    private final ListenerContainer<CacheListener> listeners = new ListenerContainer<>();
    private final AtomicBoolean isConnected = new AtomicBoolean(true);
    private final AtomicBoolean resetEventNeeded = new AtomicBoolean(true);
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
                    try
                    {
                        reset();
                    }
                    catch ( Exception e )
                    {
                        ThreadUtils.checkInterrupted(e);
                        log.error("Trying to reset after reconnection", e);
                    }
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
            try
            {
                reset();
            }
            catch ( Exception e )
            {
                ThreadUtils.checkInterrupted(e);
                // TODO
            }
        }
    };

    private enum State
    {
        LATENT,
        STARTED,
        CLOSED
    }

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

            if ( resetEventNeeded.compareAndSet(true, false) )
            {
                notifyListeners(CacheEventType.REFRESHED);
            }
        }
    };

    InternalNodeCache(CuratorFramework client, String path, CacheFilter cacheFilter, Cache<String, CachedNode> cache)
    {
        super(cache);
        this.client = client.newWatcherRemoveCuratorFramework();
        this.path = PathUtils.validatePath(path);
        this.cacheFilter = Objects.requireNonNull(cacheFilter, "cacheFilter cannot be null");
    }

    @Override
    public void start()
    {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "already started");

        client.getConnectionStateListenable().addListener(connectionStateListener);
        refreshAll();
    }

    @Override
    public void close()
    {
        if ( state.compareAndSet(State.STARTED, State.CLOSED) )
        {
            client.removeWatchers();
            listeners.clear();
            client.getConnectionStateListenable().removeListener(connectionStateListener);
        }
    }

    @Override
    public Listenable<CacheListener> getListenable()
    {
        return listeners;
    }

    @Override
    public void refreshAll()
    {
        try
        {
            resetEventNeeded.set(true);
            reset();
        }
        catch ( Exception e )
        {
            ThreadUtils.checkInterrupted(e);
            // TODO
        }
    }

    @Override
    public void refresh(String path)
    {
        Preconditions.checkArgument(this.path.equals(path), "Bad path: " + path);
        refreshAll();
    }

    private void reset() throws Exception
    {
        if ( (state.get() == State.STARTED) && isConnected.get() )
        {
            client.checkExists().usingWatcher(watcher).inBackground(backgroundCallback).forPath(path);
        }
    }

    private void processBackgroundResult(CuratorEvent event) throws Exception
    {
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
                        case IGNORE:
                        {
                            throw new UnsupportedOperationException("Single node cache does not support action: IGNORE");
                        }

                        case DO_NOT_GET_DATA:
                        {
                            setNewData(nullNode);
                            break;
                        }

                        case GET_DATA:
                        {
                            client.getData().usingWatcher(watcher).inBackground(backgroundCallback).forPath(path);
                            break;
                        }

                        case GET_COMPRESSED:
                        {
                            client.getData().decompressed().usingWatcher(watcher).inBackground(backgroundCallback).forPath(path);
                            break;
                        }
                    }
                }
                break;
            }
        }
    }

    @VisibleForTesting
    volatile Exchanger<Object> rebuildTestExchanger;
    private void setNewData(CachedNode newData) throws InterruptedException
    {
        CachedNode previousData = data.getAndSet(newData);
        if ( newData == null )
        {
            notifyListeners(CacheEventType.NODE_DELETED);
        }
        else if ( previousData == null )
        {
            notifyListeners(CacheEventType.NODE_CREATED);
        }
        else if ( !previousData.equals(newData) )
        {
            notifyListeners(CacheEventType.NODE_CHANGED);
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

    private void notifyListeners(final CacheEventType event)
    {
        listeners.forEach
        (
            new Function<CacheListener, Void>()
            {
                @Override
                public Void apply(CacheListener listener)
                {
                    try
                    {
                        listener.process(event, path);
                    }
                    catch ( Exception e )
                    {
                        ThreadUtils.checkInterrupted(e);
                        log.error("Calling listener", e);
                    }
                    return null;
                }
            }
        );
    }
}
