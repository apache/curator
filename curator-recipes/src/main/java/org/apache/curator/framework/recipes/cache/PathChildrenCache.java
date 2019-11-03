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
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.EnsureContainers;
import org.apache.curator.framework.WatcherRemoveCuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.listen.StandardListenerManager;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.utils.CloseableExecutorService;
import org.apache.curator.utils.PathUtils;
import org.apache.curator.utils.ThreadUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Exchanger;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;

/**
 * <p>A utility that attempts to keep all data from all children of a ZK path locally cached. This class
 * will watch the ZK path, respond to update/create/delete events, pull down the data, etc. You can
 * register a listener that will get notified when changes occur.</p>
 * <p></p>
 * <p><b>IMPORTANT</b> - it's not possible to stay transactionally in sync. Users of this class must
 * be prepared for false-positives and false-negatives. Additionally, always use the version number
 * when updating data to avoid overwriting another process' change.</p>
 *
 * @deprecated replace by {@link org.apache.curator.framework.recipes.cache.CuratorCache}
 */
@Deprecated
public class PathChildrenCache implements Closeable
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final WatcherRemoveCuratorFramework client;
    private final String path;
    private final CloseableExecutorService executorService;
    private final boolean cacheData;
    private final boolean dataIsCompressed;
    private final StandardListenerManager<PathChildrenCacheListener> listeners = StandardListenerManager.standard();
    private final ConcurrentMap<String, ChildData> currentData = Maps.newConcurrentMap();
    private final AtomicReference<Map<String, ChildData>> initialSet = new AtomicReference<Map<String, ChildData>>();
    private final Set<Operation> operationsQuantizer = Sets.newSetFromMap(Maps.<Operation, Boolean>newConcurrentMap());
    private final AtomicReference<State> state = new AtomicReference<State>(State.LATENT);
    private final EnsureContainers ensureContainers;

    private enum State
    {
        LATENT,
        STARTED,
        CLOSED
    }

    private static final ChildData NULL_CHILD_DATA = new ChildData("/", null, null);

    private static final boolean USE_EXISTS = Boolean.getBoolean("curator-path-children-cache-use-exists");

    private volatile Watcher childrenWatcher = new Watcher()
    {
        @Override
        public void process(WatchedEvent event)
        {
            offerOperation(new RefreshOperation(PathChildrenCache.this, RefreshMode.STANDARD));
        }
    };

    private volatile Watcher dataWatcher = new Watcher()
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
                    offerOperation(new GetDataOperation(PathChildrenCache.this, event.getPath()));
                }
            }
            catch ( Exception e )
            {
                ThreadUtils.checkInterrupted(e);
                handleException(e);
            }
        }
    };

    @VisibleForTesting
    volatile Exchanger<Object> rebuildTestExchanger;

    private volatile ConnectionStateListener connectionStateListener = new ConnectionStateListener()
    {
        @Override
        public void stateChanged(CuratorFramework client, ConnectionState newState)
        {
            handleStateChange(newState);
        }
    };
    public static final ThreadFactory defaultThreadFactory = ThreadUtils.newThreadFactory("PathChildrenCache");

    /**
     * @param client the client
     * @param path   path to watch
     * @param mode   caching mode
     * @deprecated use {@link #PathChildrenCache(CuratorFramework, String, boolean)} instead
     */
    @Deprecated
    @SuppressWarnings("deprecation")
    public PathChildrenCache(CuratorFramework client, String path, PathChildrenCacheMode mode)
    {
        this(client, path, mode != PathChildrenCacheMode.CACHE_PATHS_ONLY, false, new CloseableExecutorService(Executors.newSingleThreadExecutor(defaultThreadFactory), true));
    }

    /**
     * @param client        the client
     * @param path          path to watch
     * @param mode          caching mode
     * @param threadFactory factory to use when creating internal threads
     * @deprecated use {@link #PathChildrenCache(CuratorFramework, String, boolean, ThreadFactory)} instead
     */
    @Deprecated
    @SuppressWarnings("deprecation")
    public PathChildrenCache(CuratorFramework client, String path, PathChildrenCacheMode mode, ThreadFactory threadFactory)
    {
        this(client, path, mode != PathChildrenCacheMode.CACHE_PATHS_ONLY, false, new CloseableExecutorService(Executors.newSingleThreadExecutor(threadFactory), true));
    }

    /**
     * @param client    the client
     * @param path      path to watch
     * @param cacheData if true, node contents are cached in addition to the stat
     */
    public PathChildrenCache(CuratorFramework client, String path, boolean cacheData)
    {
        this(client, path, cacheData, false, new CloseableExecutorService(Executors.newSingleThreadExecutor(defaultThreadFactory), true));
    }

    /**
     * @param client        the client
     * @param path          path to watch
     * @param cacheData     if true, node contents are cached in addition to the stat
     * @param threadFactory factory to use when creating internal threads
     */
    public PathChildrenCache(CuratorFramework client, String path, boolean cacheData, ThreadFactory threadFactory)
    {
        this(client, path, cacheData, false, new CloseableExecutorService(Executors.newSingleThreadExecutor(threadFactory), true));
    }

    /**
     * @param client           the client
     * @param path             path to watch
     * @param cacheData        if true, node contents are cached in addition to the stat
     * @param dataIsCompressed if true, data in the path is compressed
     * @param threadFactory    factory to use when creating internal threads
     */
    public PathChildrenCache(CuratorFramework client, String path, boolean cacheData, boolean dataIsCompressed, ThreadFactory threadFactory)
    {
        this(client, path, cacheData, dataIsCompressed, new CloseableExecutorService(Executors.newSingleThreadExecutor(threadFactory), true));
    }

    /**
     * @param client           the client
     * @param path             path to watch
     * @param cacheData        if true, node contents are cached in addition to the stat
     * @param dataIsCompressed if true, data in the path is compressed
     * @param executorService  ExecutorService to use for the PathChildrenCache's background thread. This service should be single threaded, otherwise the cache may see inconsistent results.
     */
    public PathChildrenCache(CuratorFramework client, String path, boolean cacheData, boolean dataIsCompressed, final ExecutorService executorService)
    {
        this(client, path, cacheData, dataIsCompressed, new CloseableExecutorService(executorService));
    }

    /**
     * @param client           the client
     * @param path             path to watch
     * @param cacheData        if true, node contents are cached in addition to the stat
     * @param dataIsCompressed if true, data in the path is compressed
     * @param executorService  Closeable ExecutorService to use for the PathChildrenCache's background thread. This service should be single threaded, otherwise the cache may see inconsistent results.
     */
    public PathChildrenCache(CuratorFramework client, String path, boolean cacheData, boolean dataIsCompressed, final CloseableExecutorService executorService)
    {
        this.client = client.newWatcherRemoveCuratorFramework();
        this.path = PathUtils.validatePath(path);
        this.cacheData = cacheData;
        this.dataIsCompressed = dataIsCompressed;
        this.executorService = executorService;
        ensureContainers = new EnsureContainers(client, path);
    }

    /**
     * Start the cache. The cache is not started automatically. You must call this method.
     *
     * @throws Exception errors
     */
    public void start() throws Exception
    {
        start(StartMode.NORMAL);
    }

    /**
     * Same as {@link #start()} but gives the option of doing an initial build
     *
     * @param buildInitial if true, {@link #rebuild()} will be called before this method
     *                     returns in order to get an initial view of the node; otherwise,
     *                     the cache will be initialized asynchronously
     * @throws Exception errors
     * @deprecated use {@link #start(StartMode)}
     */
    @Deprecated
    public void start(boolean buildInitial) throws Exception
    {
        start(buildInitial ? StartMode.BUILD_INITIAL_CACHE : StartMode.NORMAL);
    }

    /**
     * Method of priming cache on {@link PathChildrenCache#start(StartMode)}
     */
    public enum StartMode
    {
        /**
         * The cache will be primed (in the background) with initial values.
         * Events for existing and new nodes will be posted.
         */
        NORMAL,

        /**
         * The cache will be primed (in the foreground) with initial values.
         * {@link PathChildrenCache#rebuild()} will be called before
         * the {@link PathChildrenCache#start(StartMode)} method returns
         * in order to get an initial view of the node.
         */
        BUILD_INITIAL_CACHE,

        /**
         * After cache is primed with initial values (in the background) a
         * {@link PathChildrenCacheEvent.Type#INITIALIZED} will be posted.
         */
        POST_INITIALIZED_EVENT
    }

    /**
     * Start the cache. The cache is not started automatically. You must call this method.
     *
     * @param mode Method for priming the cache
     * @throws Exception errors
     */
    public void start(StartMode mode) throws Exception
    {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "already started");
        mode = Preconditions.checkNotNull(mode, "mode cannot be null");

        client.getConnectionStateListenable().addListener(connectionStateListener);

        switch ( mode )
        {
            case NORMAL:
            {
                offerOperation(new RefreshOperation(this, RefreshMode.STANDARD));
                break;
            }

            case BUILD_INITIAL_CACHE:
            {
                rebuild();
                break;
            }

            case POST_INITIALIZED_EVENT:
            {
                initialSet.set(Maps.<String, ChildData>newConcurrentMap());
                offerOperation(new RefreshOperation(this, RefreshMode.POST_INITIALIZED));
                break;
            }
        }
    }

    /**
     * NOTE: this is a BLOCKING method. Completely rebuild the internal cache by querying
     * for all needed data WITHOUT generating any events to send to listeners.
     *
     * @throws Exception errors
     */
    public void rebuild() throws Exception
    {
        Preconditions.checkState(state.get() == State.STARTED, "cache has been closed");

        ensurePath();

        clear();

        List<String> children = client.getChildren().forPath(path);
        for ( String child : children )
        {
            String fullPath = ZKPaths.makePath(path, child);
            internalRebuildNode(fullPath);

            if ( rebuildTestExchanger != null )
            {
                rebuildTestExchanger.exchange(new Object());
            }
        }

        // this is necessary so that any updates that occurred while rebuilding are taken
        offerOperation(new RefreshOperation(this, RefreshMode.FORCE_GET_DATA_AND_STAT));
    }

    /**
     * NOTE: this is a BLOCKING method. Rebuild the internal cache for the given node by querying
     * for all needed data WITHOUT generating any events to send to listeners.
     *
     * @param fullPath full path of the node to rebuild
     * @throws Exception errors
     */
    public void rebuildNode(String fullPath) throws Exception
    {
        Preconditions.checkArgument(ZKPaths.getPathAndNode(fullPath).getPath().equals(path), "Node is not part of this cache: " + fullPath);
        Preconditions.checkState(state.get() == State.STARTED, "cache has been closed");

        ensurePath();
        internalRebuildNode(fullPath);

        // this is necessary so that any updates that occurred while rebuilding are taken
        // have to rebuild entire tree in case this node got deleted in the interim
        offerOperation(new RefreshOperation(this, RefreshMode.FORCE_GET_DATA_AND_STAT));
    }

    /**
     * Close/end the cache
     *
     * @throws IOException errors
     */
    @Override
    public void close() throws IOException
    {
        if ( state.compareAndSet(State.STARTED, State.CLOSED) )
        {
            client.getConnectionStateListenable().removeListener(connectionStateListener);
            listeners.clear();

            executorService.close();
            client.removeWatchers();

            // TODO
            // This seems to enable even more GC - I'm not sure why yet - it
            // has something to do with Guava's cache and circular references
            connectionStateListener = null;
            childrenWatcher = null;
            dataWatcher = null;
        }
    }

    /**
     * Return the cache listenable
     *
     * @return listenable
     */
    public Listenable<PathChildrenCacheListener> getListenable()
    {
        return listeners;
    }

    /**
     * Return the current data. There are no guarantees of accuracy. This is
     * merely the most recent view of the data. The data is returned in sorted order.
     *
     * @return list of children and data
     */
    public List<ChildData> getCurrentData()
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
    public ChildData getCurrentData(String fullPath)
    {
        return currentData.get(fullPath);
    }

    /**
     * As a memory optimization, you can clear the cached data bytes for a node. Subsequent
     * calls to {@link ChildData#getData()} for this node will return <code>null</code>.
     *
     * @param fullPath the path of the node to clear
     */
    public void clearDataBytes(String fullPath)
    {
        clearDataBytes(fullPath, -1);
    }

    /**
     * As a memory optimization, you can clear the cached data bytes for a node. Subsequent
     * calls to {@link ChildData#getData()} for this node will return <code>null</code>.
     *
     * @param fullPath  the path of the node to clear
     * @param ifVersion if non-negative, only clear the data if the data's version matches this version
     * @return true if the data was cleared
     */
    public boolean clearDataBytes(String fullPath, int ifVersion)
    {
        ChildData data = currentData.get(fullPath);
        if ( data != null )
        {
            if ( (ifVersion < 0) || (ifVersion == data.getStat().getVersion()) )
            {
                if ( data.getData() != null )
                {
                    currentData.replace(fullPath, data, new ChildData(data.getPath(), data.getStat(), null));
                }
                return true;
            }
        }
        return false;
    }

    /**
     * Clear out current data and begin a new query on the path
     *
     * @throws Exception errors
     */
    public void clearAndRefresh() throws Exception
    {
        currentData.clear();
        offerOperation(new RefreshOperation(this, RefreshMode.STANDARD));
    }

    /**
     * Clears the current data without beginning a new query and without generating any events
     * for listeners.
     */
    public void clear()
    {
        currentData.clear();
    }

    enum RefreshMode
    {
        STANDARD,
        FORCE_GET_DATA_AND_STAT,
        POST_INITIALIZED,
        NO_NODE_EXCEPTION
    }

    void refresh(final RefreshMode mode) throws Exception
    {
        ensurePath();

        final BackgroundCallback callback = new BackgroundCallback()
        {
            @Override
            public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
            {
                if ( reRemoveWatchersOnBackgroundClosed() )
                {
                    return;
                }
                if ( event.getResultCode() == KeeperException.Code.OK.intValue() )
                {
                    processChildren(event.getChildren(), mode);
                }
                else if ( event.getResultCode() == KeeperException.Code.NONODE.intValue() )
                {
                    if ( mode == RefreshMode.NO_NODE_EXCEPTION )
                    {
                        log.debug("KeeperException.NoNodeException received for getChildren() and refresh has failed. Resetting ensureContainers but not refreshing. Path: [{}]", path);
                        ensureContainers.reset();
                    }
                    else
                    {
                        log.debug("KeeperException.NoNodeException received for getChildren(). Resetting ensureContainers. Path: [{}]", path);
                        ensureContainers.reset();
                        offerOperation(new RefreshOperation(PathChildrenCache.this, RefreshMode.NO_NODE_EXCEPTION));
                    }
                }
            }
        };

        client.getChildren().usingWatcher(childrenWatcher).inBackground(callback).forPath(path);
    }

    void callListeners(final PathChildrenCacheEvent event)
    {
        listeners.forEach(listener -> {
            try
            {
                listener.childEvent(client, event);
            }
            catch ( Exception e )
            {
                ThreadUtils.checkInterrupted(e);
                handleException(e);
            }
        });
    }

    void getDataAndStat(final String fullPath) throws Exception
    {
        BackgroundCallback callback = new BackgroundCallback()
        {
            @Override
            public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
            {
                if ( reRemoveWatchersOnBackgroundClosed() )
                {
                    return;
                }
                applyNewData(fullPath, event.getResultCode(), event.getStat(), cacheData ? event.getData() : null);
            }
        };

        if ( USE_EXISTS && !cacheData )
        {
            client.checkExists().usingWatcher(dataWatcher).inBackground(callback).forPath(fullPath);
        }
        else
        {
            // always use getData() instead of exists() to avoid leaving unneeded watchers which is a type of resource leak
            if ( dataIsCompressed && cacheData )
            {
                client.getData().decompressed().usingWatcher(dataWatcher).inBackground(callback).forPath(fullPath);
            }
            else
            {
                client.getData().usingWatcher(dataWatcher).inBackground(callback).forPath(fullPath);
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

    protected void ensurePath() throws Exception
    {
        ensureContainers.ensure();
    }

    @VisibleForTesting
    protected void remove(String fullPath)
    {
        ChildData data = currentData.remove(fullPath);
        if ( data != null )
        {
            offerOperation(new EventOperation(this, new PathChildrenCacheEvent(PathChildrenCacheEvent.Type.CHILD_REMOVED, data)));
        }

        Map<String, ChildData> localInitialSet = initialSet.get();
        if ( localInitialSet != null )
        {
            localInitialSet.remove(ZKPaths.getNodeFromPath(fullPath));
            maybeOfferInitializedEvent(localInitialSet);
        }
    }

    private boolean reRemoveWatchersOnBackgroundClosed()
    {
        if ( state.get().equals(State.CLOSED))
        {
            client.removeWatchers();
            return true;
        }
        return false;
    }

    private void internalRebuildNode(String fullPath) throws Exception
    {
        if ( cacheData )
        {
            try
            {
                Stat stat = new Stat();
                byte[] bytes = dataIsCompressed ? client.getData().decompressed().storingStatIn(stat).forPath(fullPath) : client.getData().storingStatIn(stat).forPath(fullPath);
                currentData.put(fullPath, new ChildData(fullPath, stat, bytes));
            }
            catch ( KeeperException.NoNodeException ignore )
            {
                // node no longer exists - remove it
                currentData.remove(fullPath);
            }
        }
        else
        {
            Stat stat = client.checkExists().forPath(fullPath);
            if ( stat != null )
            {
                currentData.put(fullPath, new ChildData(fullPath, stat, null));
            }
            else
            {
                // node no longer exists - remove it
                currentData.remove(fullPath);
            }
        }
    }

    private void handleStateChange(ConnectionState newState)
    {
        switch ( newState )
        {
        case SUSPENDED:
        {
            offerOperation(new EventOperation(this, new PathChildrenCacheEvent(PathChildrenCacheEvent.Type.CONNECTION_SUSPENDED, null)));
            break;
        }

        case LOST:
        {
            offerOperation(new EventOperation(this, new PathChildrenCacheEvent(PathChildrenCacheEvent.Type.CONNECTION_LOST, null)));
            break;
        }

        case CONNECTED:
        case RECONNECTED:
        {
            try
            {
                offerOperation(new RefreshOperation(this, RefreshMode.FORCE_GET_DATA_AND_STAT));
                offerOperation(new EventOperation(this, new PathChildrenCacheEvent(PathChildrenCacheEvent.Type.CONNECTION_RECONNECTED, null)));
            }
            catch ( Exception e )
            {
                ThreadUtils.checkInterrupted(e);
                handleException(e);
            }
            break;
        }
        }
    }

    private void processChildren(List<String> children, RefreshMode mode) throws Exception
    {
        Set<String> removedNodes = Sets.newHashSet(currentData.keySet());
        for ( String child : children ) {
            removedNodes.remove(ZKPaths.makePath(path, child));
        }

        for ( String fullPath : removedNodes )
        {
            remove(fullPath);
        }

        for ( String name : children )
        {
            String fullPath = ZKPaths.makePath(path, name);

            if ( (mode == RefreshMode.FORCE_GET_DATA_AND_STAT) || !currentData.containsKey(fullPath) )
            {
                getDataAndStat(fullPath);
            }

            updateInitialSet(name, NULL_CHILD_DATA);
        }
        maybeOfferInitializedEvent(initialSet.get());
    }

    private void applyNewData(String fullPath, int resultCode, Stat stat, byte[] bytes)
    {
        if ( resultCode == KeeperException.Code.OK.intValue() ) // otherwise - node must have dropped or something - we should be getting another event
        {
            ChildData data = new ChildData(fullPath, stat, bytes);
            ChildData previousData = currentData.put(fullPath, data);
            if ( previousData == null ) // i.e. new
            {
                offerOperation(new EventOperation(this, new PathChildrenCacheEvent(PathChildrenCacheEvent.Type.CHILD_ADDED, data)));
            }
            else if ( stat.getMzxid() != previousData.getStat().getMzxid() )
            {
                offerOperation(new EventOperation(this, new PathChildrenCacheEvent(PathChildrenCacheEvent.Type.CHILD_UPDATED, data)));
            }
            updateInitialSet(ZKPaths.getNodeFromPath(fullPath), data);
        }
        else if ( resultCode == KeeperException.Code.NONODE.intValue() )
        {
            log.debug("NoNode at path {}, removing child from initialSet", fullPath);
            remove(fullPath);
        }
    }

    private void updateInitialSet(String name, ChildData data)
    {
        Map<String, ChildData> localInitialSet = initialSet.get();
        if ( localInitialSet != null )
        {
            localInitialSet.put(name, data);
            maybeOfferInitializedEvent(localInitialSet);
        }
    }

    private void maybeOfferInitializedEvent(Map<String, ChildData> localInitialSet)
    {
        if ( !hasUninitialized(localInitialSet) )
        {
            // all initial children have been processed - send initialized message

            if ( initialSet.getAndSet(null) != null )   // avoid edge case - don't send more than 1 INITIALIZED event
            {
                final List<ChildData> children = ImmutableList.copyOf(localInitialSet.values());
                PathChildrenCacheEvent event = new PathChildrenCacheEvent(PathChildrenCacheEvent.Type.INITIALIZED, null)
                {
                    @Override
                    public List<ChildData> getInitialData()
                    {
                        return children;
                    }
                };
                offerOperation(new EventOperation(this, event));
            }
        }
    }

    private boolean hasUninitialized(Map<String, ChildData> localInitialSet)
    {
        if ( localInitialSet == null )
        {
            return false;
        }

        Map<String, ChildData> uninitializedChildren = Maps.filterValues
            (
                localInitialSet,
                new Predicate<ChildData>()
                {
                    @Override
                    public boolean apply(ChildData input)
                    {
                        return (input == NULL_CHILD_DATA);  // check against ref intentional
                    }
                }
            );
        return (uninitializedChildren.size() != 0);
    }

    void offerOperation(final Operation operation)
    {
        if ( operationsQuantizer.add(operation) )
        {
            submitToExecutor
            (
                new Runnable()
                {
                    @Override
                    public void run()
                    {
                        try
                        {
                            operationsQuantizer.remove(operation);
                            operation.invoke();
                        }
                        catch ( InterruptedException e )
                        {
                            //We expect to get interrupted during shutdown,
                            //so just ignore these events
                            if ( state.get() != State.CLOSED )
                            {
                                handleException(e);
                            }
                            Thread.currentThread().interrupt();
                        }
                        catch ( Exception e )
                        {
                            ThreadUtils.checkInterrupted(e);
                            handleException(e);
                        }
                    }
                }
            );
        }
    }

    /**
     * Submits a runnable to the executor.
     * <p>
     * This method is synchronized because it has to check state about whether this instance is still open.  Without this check
     * there is a race condition with the dataWatchers that get set.  Even after this object is closed() it can still be
     * called by those watchers, because the close() method cannot actually disable the watcher.
     * <p>
     * The synchronization overhead should be minimal if non-existant as this is generally only called from the
     * ZK client thread and will only contend if close() is called in parallel with an update, and that's the exact state
     * we want to protect from.
     *
     * @param command The runnable to run
     */
    private synchronized void submitToExecutor(final Runnable command)
    {
        if ( state.get() == State.STARTED )
        {
            executorService.submit(command);
        }
    }
}
