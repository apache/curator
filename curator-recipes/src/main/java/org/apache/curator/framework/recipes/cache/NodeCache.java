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
package org.apache.curator.framework.recipes.cache;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.WatcherRemoveCuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.listen.StandardListenerManager;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.utils.PathUtils;
import org.apache.curator.utils.ThreadUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Exchanger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * <p>A utility that attempts to keep the data from a node locally cached. This class
 * will watch the node, respond to update/create/delete events, pull down the data, etc. You can
 * register a listener that will get notified when changes occur.</p>
 *
 * <p><b>IMPORTANT</b> - it's not possible to stay transactionally in sync. Users of this class must
 * be prepared for false-positives and false-negatives. Additionally, always use the version number
 * when updating data to avoid overwriting another process' change.</p>
 *
 * @deprecated replace by {@link org.apache.curator.framework.recipes.cache.CuratorCache}
 */
@Deprecated
public class NodeCache implements Closeable
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final WatcherRemoveCuratorFramework client;
    private final String path;
    private final boolean dataIsCompressed;
    private final AtomicReference<ChildData> data = new AtomicReference<ChildData>(null);
    private final AtomicReference<State> state = new AtomicReference<State>(State.LATENT);
    private final StandardListenerManager<NodeCacheListener> listeners = StandardListenerManager.standard();
    private final AtomicBoolean isConnected = new AtomicBoolean(true);
    private ConnectionStateListener connectionStateListener = new ConnectionStateListener()
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
            catch(Exception e)
            {
                ThreadUtils.checkInterrupted(e);
                handleException(e);
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
            processBackgroundResult(event);
        }
    };

    /**
     * @param client curator client
     * @param path the full path to the node to cache
     */
    public NodeCache(CuratorFramework client, String path)
    {
        this(client, path, false);
    }

    /**
     * @param client curator client
     * @param path the full path to the node to cache
     * @param dataIsCompressed if true, data in the path is compressed
     */
    public NodeCache(CuratorFramework client, String path, boolean dataIsCompressed)
    {
        this.client = client.newWatcherRemoveCuratorFramework();
        this.path = PathUtils.validatePath(path);
        this.dataIsCompressed = dataIsCompressed;
    }

    public CuratorFramework getClient()
    {
        return client;
    }

    /**
     * Start the cache. The cache is not started automatically. You must call this method.
     *
     * @throws Exception errors
     */
    public void     start() throws Exception
    {
        start(false);
    }

    /**
     * Same as {@link #start()} but gives the option of doing an initial build
     *
     * @param buildInitial if true, {@link #rebuild()} will be called before this method
     *                     returns in order to get an initial view of the node
     * @throws Exception errors
     */
    public void     start(boolean buildInitial) throws Exception
    {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "Cannot be started more than once");

        client.getConnectionStateListenable().addListener(connectionStateListener);

        if ( buildInitial )
        {
            client.checkExists().creatingParentContainersIfNeeded().forPath(path);
            internalRebuild();
        }
        reset();
    }

    @Override
    public void close() throws IOException
    {
        if ( state.compareAndSet(State.STARTED, State.CLOSED) )
        {
            client.removeWatchers();
            listeners.clear();
            client.getConnectionStateListenable().removeListener(connectionStateListener);

            // TODO
            // From PathChildrenCache
            // This seems to enable even more GC - I'm not sure why yet - it
            // has something to do with Guava's cache and circular references
            connectionStateListener = null;
            watcher = null;
        }        
    }

    /**
     * Return the cache listenable
     *
     * @return listenable
     */
    public Listenable<NodeCacheListener> getListenable()
    {
        Preconditions.checkState(state.get() != State.CLOSED, "Closed");

        return listeners;
    }

    /**
     * NOTE: this is a BLOCKING method. Completely rebuild the internal cache by querying
     * for all needed data WITHOUT generating any events to send to listeners.
     *
     * @throws Exception errors
     */
    public void     rebuild() throws Exception
    {
        Preconditions.checkState(state.get() == State.STARTED, "Not started");

        internalRebuild();

        reset();
    }

    /**
     * Return the current data. There are no guarantees of accuracy. This is
     * merely the most recent view of the data. If the node does not exist,
     * this returns null
     *
     * @return data or null
     */
    public ChildData getCurrentData()
    {
        return data.get();
    }

    /**
     * Return the path this cache is watching
     *
     * @return path
     */
    public String getPath()
    {
        return path;
    }

    @VisibleForTesting
    volatile Exchanger<Object> rebuildTestExchanger;

    private void     reset() throws Exception
    {
        if ( (state.get() == State.STARTED) && isConnected.get() )
        {
            client.checkExists().creatingParentContainersIfNeeded().usingWatcher(watcher).inBackground(backgroundCallback).forPath(path);
        }
    }

    private void     internalRebuild() throws Exception
    {
        try
        {
            Stat    stat = new Stat();
            byte[]  bytes = dataIsCompressed ? client.getData().decompressed().storingStatIn(stat).forPath(path) : client.getData().storingStatIn(stat).forPath(path);
            data.set(new ChildData(path, stat, bytes));
        }
        catch ( KeeperException.NoNodeException e )
        {
            data.set(null);
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
                    ChildData childData = new ChildData(path, event.getStat(), event.getData());
                    setNewData(childData);
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
                    if ( dataIsCompressed )
                    {
                        client.getData().decompressed().usingWatcher(watcher).inBackground(backgroundCallback).forPath(path);
                    }
                    else
                    {
                        client.getData().usingWatcher(watcher).inBackground(backgroundCallback).forPath(path);
                    }
                }
                break;
            }
        }
    }

    private void setNewData(ChildData newData) throws InterruptedException
    {
        ChildData   previousData = data.getAndSet(newData);
        if ( !Objects.equal(previousData, newData) )
        {
            listeners.forEach(listener -> {
                try
                {
                    listener.nodeChanged();
                }
                catch ( Exception e )
                {
                    ThreadUtils.checkInterrupted(e);
                    log.error("Calling listener", e);
                }
            });

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
    
    /**
     * Default behavior is just to log the exception
     *
     * @param e the exception
     */
    protected void handleException(Throwable e)
    {
        log.error("", e);
    }
}
