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
package com.netflix.curator.framework.recipes.cache;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.BackgroundCallback;
import com.netflix.curator.framework.api.CuratorEvent;
import com.netflix.curator.framework.listen.ListenerContainer;
import com.netflix.curator.framework.state.ConnectionState;
import com.netflix.curator.framework.state.ConnectionStateListener;
import com.netflix.curator.utils.EnsurePath;
import com.netflix.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Exchanger;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;

/**
 * <p>A utility that attempts to keep all data from all children of a ZK path locally cached. This class
 * will watch the ZK path, respond to update/create/delete events, pull down the data, etc. You can
 * register a listener that will get notified when changes occur.</p>
 *
 * <p><b>IMPORTANT</b> - it's not possible to stay transactionally in sync. Users of this class must
 * be prepared for false-positives and false-negatives. Additionally, always use the version number
 * when updating data to avoid overwriting another process' change.</p>
 */
public class PathChildrenCache implements Closeable
{
    private final Logger                    log = LoggerFactory.getLogger(getClass());
    private final CuratorFramework          client;
    private final String                    path;
    private final ExecutorService           executorService;
    private final boolean                   cacheData;
    private final boolean                   dataIsCompressed;
    private final EnsurePath                ensurePath;

    private final Watcher     childrenWatcher = new Watcher()
    {
        @Override
        public void process(WatchedEvent event)
        {
            try
            {
                refresh(false);
            }
            catch ( Exception e )
            {
                handleException(e);
            }
        }
    };

    private final Watcher     dataWatcher = new Watcher()
    {
        @Override
        public void process(WatchedEvent event)
        {
            try
            {
                if ( event.getType() == Event.EventType.NodeDeleted )
                {
                    remove(event.getPath());
                }
                else if ( event.getType() == Event.EventType.NodeDataChanged )
                {
                    getDataAndStat(event.getPath());
                }
            }
            catch ( Exception e )
            {
                handleException(e);
            }
        }
    };

    private final BlockingQueue<PathChildrenCacheEvent>         listenerEvents = new LinkedBlockingQueue<PathChildrenCacheEvent>();
    private final ListenerContainer<PathChildrenCacheListener>  listeners = new ListenerContainer<PathChildrenCacheListener>();
    private final ConcurrentMap<String, ChildData>              currentData = Maps.newConcurrentMap();
    
    @VisibleForTesting
    volatile Exchanger<Object>      rebuildTestExchanger;

    private final ConnectionStateListener connectionStateListener = new ConnectionStateListener()
    {
        @Override
        public void stateChanged(CuratorFramework client, ConnectionState newState)
        {
            handleStateChange(newState);
        }
    };
    private static final ThreadFactory defaultThreadFactory = new ThreadFactoryBuilder().setNameFormat("PathChildrenCache-%d").build();

    /**
     * @param client the client
     * @param path path to watch
     * @param mode caching mode
     *             
     * @deprecated use {@link #PathChildrenCache(CuratorFramework, String, boolean)} instead
     */
    @SuppressWarnings("deprecation")
    public PathChildrenCache(CuratorFramework client, String path, PathChildrenCacheMode mode)
    {
        this(client, path, mode != PathChildrenCacheMode.CACHE_PATHS_ONLY, false, defaultThreadFactory);
    }

    /**
     * @param client the client
     * @param path path to watch
     * @param mode caching mode
     * @param threadFactory factory to use when creating internal threads
     *
     * @deprecated use {@link #PathChildrenCache(CuratorFramework, String, boolean, ThreadFactory)} instead
     */
    @SuppressWarnings("deprecation")
    public PathChildrenCache(CuratorFramework client, String path, PathChildrenCacheMode mode, ThreadFactory threadFactory)
    {
        this(client, path, mode != PathChildrenCacheMode.CACHE_PATHS_ONLY, false, threadFactory);
    }

    /**
     * @param client the client
     * @param path path to watch
     * @param cacheData if true, node contents are cached in addition to the stat
     */
    public PathChildrenCache(CuratorFramework client, String path, boolean cacheData)
    {
        this(client, path, cacheData, false, defaultThreadFactory);
    }

    /**
     * @param client the client
     * @param path path to watch
     * @param cacheData if true, node contents are cached in addition to the stat
     * @param threadFactory factory to use when creating internal threads
     */
    public PathChildrenCache(CuratorFramework client, String path, boolean cacheData, ThreadFactory threadFactory)
    {
        this(client, path, cacheData, false, threadFactory);
    }

    /**
     * @param client the client
     * @param path path to watch
     * @param cacheData if true, node contents are cached in addition to the stat
     * @param dataIsCompressed if true, data in the path is compressed
     * @param threadFactory factory to use when creating internal threads
     */
    public PathChildrenCache(CuratorFramework client, String path, boolean cacheData, boolean dataIsCompressed, ThreadFactory threadFactory)
    {
        this.client = client;
        this.path = path;
        this.cacheData = cacheData;
        this.dataIsCompressed = dataIsCompressed;
        executorService = Executors.newFixedThreadPool(1, threadFactory);
        ensurePath = client.newNamespaceAwareEnsurePath(path);
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
        Preconditions.checkState(!executorService.isShutdown(), "already started");

        client.getConnectionStateListenable().addListener(connectionStateListener);
        executorService.submit
        (
            new Callable<Object>()
            {
                @Override
                public Object call() throws Exception
                {
                    listenerLoop();
                    return null;
                }
            }
        );

        if ( buildInitial )
        {
            rebuild();
        }
        else
        {
            refresh(false);
        }
    }

    /**
     * NOTE: this is a BLOCKING method. Completely rebuild the internal cache by querying
     * for all needed data WITHOUT generating any events to send to listeners.
     *
     * @throws Exception errors
     */
    public void     rebuild() throws Exception
    {
        Preconditions.checkState(!executorService.isShutdown(), "cache has been closed");

        ensurePath.ensure(client.getZookeeperClient());

        List<String>            children = client.getChildren().forPath(path);
        for ( String child : children )
        {
            String  fullPath = ZKPaths.makePath(path, child);
            if ( cacheData )
            {
                try
                {
                    Stat        stat = new Stat();
                    byte[]      bytes = dataIsCompressed ? client.getData().decompressed().storingStatIn(stat).forPath(fullPath) : client.getData().storingStatIn(stat).forPath(fullPath);
                    currentData.put(fullPath, new ChildData(fullPath, stat, bytes));
                }
                catch ( KeeperException.NoNodeException ignore )
                {
                    // ignore
                }
            }
            else
            {
                Stat        stat = client.checkExists().forPath(fullPath);
                if ( stat != null )
                {
                    currentData.put(fullPath, new ChildData(fullPath, stat, null));
                }
            }

            if ( rebuildTestExchanger != null )
            {
                rebuildTestExchanger.exchange(new Object());
            }
        }

        // this is necessary so that any updates that occurred while rebuilding are taken
        refresh(true);
    }

    /**
     * Close/end the cache
     *
     * @throws IOException errors
     */
    @Override
    public void close() throws IOException
    {
        Preconditions.checkState(!executorService.isShutdown(), "has not been started");

        client.getConnectionStateListenable().removeListener(connectionStateListener);
        executorService.shutdownNow();
    }

    /**
     * Return the cache listenable
     *
     * @return listenable
     */
    public ListenerContainer<PathChildrenCacheListener> getListenable()
    {
        return listeners;
    }

    /**
     * Return the current data. There are no guarantees of accuracy. This is
     * merely the most recent view of the data. The data is returned in sorted order.
     *
     * @return list of children and data
     */
    public List<ChildData>      getCurrentData()
    {
        return ImmutableList.copyOf(Sets.<ChildData>newTreeSet(currentData.values()));
    }

    /**
     * Return the current data for the given path. There are no guarantees of accuracy. This is
     * merely the most recent view of the data. If there is no child with that path, <code>null</code>
     * is returned.
     *
     * @param fullPath full path to the node to check
     * @return data or null
     */
    public ChildData            getCurrentData(String fullPath)
    {
        return currentData.get(fullPath);
    }

    /**
     * Clear out current data and begin a new query on the path
     *
     * @throws Exception errors
     */
    public void clearAndRefresh() throws Exception
    {
        currentData.clear();
        refresh(false);
    }

    /**
     * Default behavior is just to log the exception
     *
     * @param e the exception
     */
    protected void      handleException(Throwable e)
    {
        log.error("", e);
    }

    private void handleStateChange(ConnectionState newState)
    {
        switch ( newState )
        {
            case SUSPENDED:
            {
                currentData.clear();
                listenerEvents.offer(new PathChildrenCacheEvent(PathChildrenCacheEvent.Type.RESET, null));
                break;
            }

            case LOST:
            case RECONNECTED:
            {
                try
                {
                    clearAndRefresh();
                    listenerEvents.offer(new PathChildrenCacheEvent(PathChildrenCacheEvent.Type.RESET, null));
                }
                catch ( Exception e )
                {
                    handleException(e);
                }
                break;
            }
        }
    }

    private void refresh(final boolean forceGetDataAndStat) throws Exception
    {
        ensurePath.ensure(client.getZookeeperClient());

        final BackgroundCallback  callback = new BackgroundCallback()
        {
            @Override
            public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
            {
                processChildren(event.getChildren(), forceGetDataAndStat);
            }
        };

        client.getChildren().usingWatcher(childrenWatcher).inBackground(callback).forPath(path);
    }

    private void processChildren(List<String> children, boolean forceGetDataAndStat) throws Exception
    {
        List<String>    fullPaths = Lists.transform
            (
                children,
                new Function<String, String>()
                {
                    @Override
                    public String apply(String child)
                    {
                        return ZKPaths.makePath(path, child);
                    }
                }
            );
        Set<String>     removedNodes = Sets.newHashSet(currentData.keySet());
        removedNodes.removeAll(fullPaths);
        
        for ( String fullPath : removedNodes )
        {
            remove(fullPath);
        }
        
        for ( String name : children )
        {
            String      fullPath = ZKPaths.makePath(path, name);
            if ( forceGetDataAndStat || !currentData.containsKey(fullPath) )
            {
                getDataAndStat(fullPath);
            }
        }
    }

    private void remove(String fullPath)
    {
        ChildData data = currentData.remove(fullPath);
        if ( data != null )
        {
            listenerEvents.offer(new PathChildrenCacheEvent(PathChildrenCacheEvent.Type.CHILD_REMOVED, data));
        }
    }

    private void applyNewData(String fullPath, int resultCode, Stat stat, byte[] bytes)
    {
        if ( resultCode == KeeperException.Code.OK.intValue() ) // otherwise - node must have dropped or something - we should be getting another event
        {
            ChildData       data = new ChildData(fullPath, stat, bytes);
            ChildData       previousData = currentData.put(fullPath, data);
            if ( previousData == null ) // i.e. new
            {
                listenerEvents.offer(new PathChildrenCacheEvent(PathChildrenCacheEvent.Type.CHILD_ADDED, data));
            }
            else if ( previousData.getStat().getVersion() != stat.getVersion() )
            {
                listenerEvents.offer(new PathChildrenCacheEvent(PathChildrenCacheEvent.Type.CHILD_UPDATED, data));
            }
        }
    }

    private void getDataAndStat(final String fullPath) throws Exception
    {
        BackgroundCallback  existsCallback = new BackgroundCallback()
        {
            @Override
            public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
            {
                applyNewData(fullPath, event.getResultCode(), event.getStat(), null);
            }
        };

        BackgroundCallback  getDataCallback = new BackgroundCallback()
        {
            @Override
            public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
            {
                applyNewData(fullPath, event.getResultCode(), event.getStat(), event.getData());
            }
        };

        if ( cacheData )
        {
            if ( dataIsCompressed )
            {
                client.getData().decompressed().usingWatcher(dataWatcher).inBackground(getDataCallback).forPath(fullPath);
            }
            else
            {
                client.getData().usingWatcher(dataWatcher).inBackground(getDataCallback).forPath(fullPath);
            }
        }
        else
        {
            client.checkExists().inBackground(existsCallback).forPath(fullPath);
        }
    }

    private void listenerLoop()
    {
        while ( !Thread.currentThread().isInterrupted() )
        {
            try
            {
                PathChildrenCacheEvent event = listenerEvents.take();
                callListeners(event);
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void callListeners(final PathChildrenCacheEvent event)
    {
        listeners.forEach
        (
            new Function<PathChildrenCacheListener, Void>()
            {
                @Override
                public Void apply(PathChildrenCacheListener listener)
                {
                    try
                    {
                        listener.childEvent(client, event);
                    }
                    catch ( Exception e )
                    {
                        handleException(e);
                    }
                    return null;
                }
            }
        );
    }
}
