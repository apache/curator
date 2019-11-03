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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.WatcherRemoveCuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.BackgroundPathable;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.Pathable;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.api.Watchable;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.listen.StandardListenerManager;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.curator.utils.PathUtils.validatePath;

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
public class TreeCache implements Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(TreeCache.class);
    private final boolean createParentNodes;
    private final boolean disableZkWatches;
    private final TreeCacheSelector selector;

    public static final class Builder
    {
        private final CuratorFramework client;
        private final String path;
        private boolean cacheData = true;
        private boolean dataIsCompressed = false;
        private ExecutorService executorService = null;
        private int maxDepth = Integer.MAX_VALUE;
        private boolean createParentNodes = false;
        private boolean disableZkWatches = false;
        private TreeCacheSelector selector = new DefaultTreeCacheSelector();

        private Builder(CuratorFramework client, String path)
        {
            this.client = checkNotNull(client);
            this.path = validatePath(path);
        }

        /**
         * Builds the {@link TreeCache} based on configured values.
         */
        public TreeCache build()
        {
            ExecutorService executor = executorService;
            if ( executor == null )
            {
                executor = Executors.newSingleThreadExecutor(defaultThreadFactory);
            }
            return new TreeCache(client, path, cacheData, dataIsCompressed, maxDepth, executor, createParentNodes, disableZkWatches, selector);
        }

        /**
         * Sets whether or not to cache byte data per node; default {@code true}.
         */
        public Builder setCacheData(boolean cacheData)
        {
            this.cacheData = cacheData;
            return this;
        }

        /**
         * Sets whether or to decompress node data; default {@code false}.
         */
        public Builder setDataIsCompressed(boolean dataIsCompressed)
        {
            this.dataIsCompressed = dataIsCompressed;
            return this;
        }

        /**
         * Sets the executor to publish events; a default executor will be created if not specified.
         */
        public Builder setExecutor(ThreadFactory threadFactory)
        {
            return setExecutor(Executors.newSingleThreadExecutor(threadFactory));
        }

        /**
         * Sets the executor to publish events; a default executor will be created if not specified.
         */
        public Builder setExecutor(ExecutorService executorService)
        {
            this.executorService = checkNotNull(executorService);
            return this;
        }

        /**
         * Sets the maximum depth to explore/watch.  A {@code maxDepth} of {@code 0} will watch only
         * the root node (like {@link NodeCache}); a {@code maxDepth} of {@code 1} will watch the
         * root node and its immediate children (kind of like {@link PathChildrenCache}.
         * Default: {@code Integer.MAX_VALUE}
         */
        public Builder setMaxDepth(int maxDepth)
        {
            this.maxDepth = maxDepth;
            return this;
        }

        /**
         * By default, TreeCache does not auto-create parent nodes for the cached path. Change
         * this behavior with this method. NOTE: parent nodes are created as containers
         *
         * @param createParentNodes true to create parent nodes
         * @return this for chaining
         */
        public Builder setCreateParentNodes(boolean createParentNodes)
        {
            this.createParentNodes = createParentNodes;
            return this;
        }

        /**
         * By default, TreeCache creates {@link org.apache.zookeeper.ZooKeeper} watches for every created path.
         * Change this behavior with this method.
         * @param disableZkWatches true to disable zk watches
         * @return this for chaining
         */
        public Builder disableZkWatches(boolean disableZkWatches)
        {
            this.disableZkWatches = disableZkWatches;
            return this;
        }

        /**
         * By default, {@link DefaultTreeCacheSelector} is used. Change the selector here.
         *
         * @param selector new selector
         * @return this for chaining
         */
        public Builder setSelector(TreeCacheSelector selector)
        {
            this.selector = selector;
            return this;
        }
    }

    /**
     * Create a TreeCache builder for the given client and path to configure advanced options.
     * <p/>
     * If the client is namespaced, all operations on the resulting TreeCache will be in terms of
     * the namespace, including all published events.  The given path is the root at which the
     * TreeCache will watch and explore.  If no node exists at the given path, the TreeCache will
     * be initially empty.
     *
     * @param client the client to use; may be namespaced
     * @param path   the path to the root node to watch/explore; this path need not actually exist on
     *               the server
     * @return a new builder
     */
    public static Builder newBuilder(CuratorFramework client, String path)
    {
        return new Builder(client, path);
    }

    private static final ChildData DEAD = new ChildData("/", null, null);

    private static boolean isLive(ChildData cd)
    {
        return cd != null && cd != DEAD;
    }

    private static final AtomicReferenceFieldUpdater<TreeNode, ChildData> childDataUpdater = AtomicReferenceFieldUpdater.newUpdater(TreeNode.class, ChildData.class, "childData");

    private static final AtomicReferenceFieldUpdater<TreeNode, ConcurrentMap<String, TreeNode>> childrenUpdater = (AtomicReferenceFieldUpdater)AtomicReferenceFieldUpdater.newUpdater(TreeNode.class, ConcurrentMap.class, "children");

    private final class TreeNode implements Watcher, BackgroundCallback
    {
        volatile ChildData childData;
        final TreeNode parent;
        final String path;
        volatile ConcurrentMap<String, TreeNode> children;
        final int depth;

        TreeNode(String path, TreeNode parent)
        {
            this.path = path;
            this.parent = parent;
            this.depth = parent == null ? 0 : parent.depth + 1;
        }

        private void refresh() throws Exception
        {
            if ((depth < maxDepth) && selector.traverseChildren(path))
            {
                outstandingOps.addAndGet(2);
                doRefreshData();
                doRefreshChildren();
            } else {
                refreshData();
            }
        }

        private void refreshChildren() throws Exception
        {
            if ((depth < maxDepth) && selector.traverseChildren(path))
            {
                outstandingOps.incrementAndGet();
                doRefreshChildren();
            }
        }

        private void refreshData() throws Exception
        {
            outstandingOps.incrementAndGet();
            doRefreshData();
        }

        private void doRefreshChildren() throws Exception
        {
            if ( treeState.get() == TreeState.STARTED )
            {
                maybeWatch(client.getChildren()).forPath(path);
            }
        }

        private void doRefreshData() throws Exception
        {
            if ( treeState.get() == TreeState.STARTED )
            {
                if ( dataIsCompressed )
                {
                    maybeWatch(client.getData().decompressed()).forPath(path);
                }
                else
                {
                    maybeWatch(client.getData()).forPath(path);
                }
            }
        }

        private <T, P extends Watchable<BackgroundPathable<T>> & BackgroundPathable<T>> Pathable<T> maybeWatch(
            P dataBuilder) {
            if (disableZkWatches) {
                return dataBuilder.inBackground(this);
            } else {
                return dataBuilder.usingWatcher(this).inBackground(this);
            }
        }

        void wasReconnected() throws Exception
        {
            refresh();
            ConcurrentMap<String, TreeNode> childMap = children;
            if ( childMap != null )
            {
                for ( TreeNode child : childMap.values() )
                {
                    child.wasReconnected();
                }
            }
        }

        void wasCreated() throws Exception
        {
            refresh();
        }

        void wasDeleted() throws Exception
        {
            ChildData oldChildData = childDataUpdater.getAndSet(this, DEAD);
            if ( oldChildData == DEAD )
            {
                return;
            }
            ConcurrentMap<String, TreeNode> childMap = childrenUpdater.getAndSet(this, null);
            if ( childMap != null )
            {
                ArrayList<TreeNode> childCopy = new ArrayList<TreeNode>(childMap.values());
                childMap.clear();
                for ( TreeNode child : childCopy )
                {
                    child.wasDeleted();
                }
            }

            if ( treeState.get() == TreeState.CLOSED )
            {
                return;
            }

            if ( isLive(oldChildData) )
            {
                publishEvent(TreeCacheEvent.Type.NODE_REMOVED, oldChildData);
            }

            if ( parent == null )
            {
                // Root node; use an exist query to watch for existence.
                maybeWatch(client.checkExists()).forPath(path);
            }
            else
            {
                // Remove from parent if we're currently a child
                ConcurrentMap<String, TreeNode> parentChildMap = parent.children;
                if ( parentChildMap != null )
                {
                    parentChildMap.remove(ZKPaths.getNodeFromPath(path), this);
                }
            }
        }

        @Override
        public void process(WatchedEvent event)
        {
            LOG.debug("process: {}", event);
            try
            {
                switch ( event.getType() )
                {
                case NodeCreated:
                    Preconditions.checkState(parent == null, "unexpected NodeCreated on non-root node");
                    wasCreated();
                    break;
                case NodeChildrenChanged:
                    refreshChildren();
                    break;
                case NodeDataChanged:
                    refreshData();
                    break;
                case NodeDeleted:
                    wasDeleted();
                    break;
                }
            }
            catch ( Exception e )
            {
                ThreadUtils.checkInterrupted(e);
                handleException(e);
            }
        }

        @Override
        public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
        {
            LOG.debug("processResult: {}", event);
            Stat newStat = event.getStat();
            switch ( event.getType() )
            {
            case EXISTS:
                Preconditions.checkState(parent == null, "unexpected EXISTS on non-root node");
                if ( event.getResultCode() == KeeperException.Code.OK.intValue() )
                {
                    childDataUpdater.compareAndSet(this, DEAD, null);
                    wasCreated();
                }
                break;
            case CHILDREN:
                if ( event.getResultCode() == KeeperException.Code.OK.intValue() )
                {
                    ChildData oldChildData = childData;
                    //TODO consider doing update of cversion, pzxid, numChildren only
                    if ( isLive(oldChildData) && oldChildData.getStat().getMzxid() == newStat.getMzxid() )
                    {
                        // Only update stat if mzxid is same, otherwise we might obscure
                        // GET_DATA event updates.
                        childDataUpdater.compareAndSet(this, oldChildData, new ChildData(oldChildData.getPath(), newStat, oldChildData.getData()));
                    }

                    if ( event.getChildren().isEmpty() )
                    {
                        break;
                    }

                    ConcurrentMap<String, TreeNode> childMap = children;
                    while ( childMap == null )
                    {
                        childMap = Maps.newConcurrentMap();
                        if ( !childrenUpdater.compareAndSet(this, null, childMap) )
                        {
                            childMap = children;
                        }
                    }

                    // Present new children in sorted order for test determinism.
                    List<String> newChildren = new ArrayList<String>();
                    for ( String child : event.getChildren() )
                    {
                        if ( !childMap.containsKey(child) && selector.acceptChild(ZKPaths.makePath(path, child)) )
                        {
                            newChildren.add(child);
                        }
                    }

                    Collections.sort(newChildren);
                    for ( String child : newChildren )
                    {
                        String fullPath = ZKPaths.makePath(path, child);
                        TreeNode node = new TreeNode(fullPath, this);
                        if ( childMap.putIfAbsent(child, node) == null )
                        {
                            node.wasCreated();
                        }
                    }
                }
                else if ( event.getResultCode() == KeeperException.Code.NONODE.intValue() )
                {
                    wasDeleted();
                }
                break;
            case GET_DATA:
                if ( event.getResultCode() == KeeperException.Code.OK.intValue() )
                {
                    String eventPath = event.getPath();
                    ChildData toPublish = new ChildData(eventPath, newStat, event.getData());
                    ChildData toUpdate = cacheData ? toPublish : new ChildData(eventPath, newStat, null);
                    while ( true )
                    {
                        final ChildData oldChildData = childData;
                        // Ignore this event if we've already processed a newer update for this node.
                        if ( isLive(oldChildData) && newStat.getMzxid() <= oldChildData.getStat().getMzxid() )
                        {
                            break;
                        }
                        // Non-root nodes are not allowed to transition from dead -> live;
                        // make sure this isn't a delayed response that came in after death.
                        if ( parent != null && oldChildData == DEAD )
                        {
                            break;
                        }
                        if ( childDataUpdater.compareAndSet(this, oldChildData, toUpdate) )
                        {
                            publishEvent(isLive(oldChildData) ? TreeCacheEvent.Type.NODE_UPDATED : TreeCacheEvent.Type.NODE_ADDED, toPublish);
                            break;
                        }
                    }
                }
                else if ( event.getResultCode() == KeeperException.Code.NONODE.intValue() )
                {
                    wasDeleted();
                }
                break;
            default:
                // An unknown event, probably an error of some sort like connection loss.
                LOG.info(String.format("Unknown event %s", event));
                // Don't produce an initialized event on error; reconnect can fix this.
                outstandingOps.decrementAndGet();
                return;
            }

            if ( outstandingOps.decrementAndGet() == 0 )
            {
                if ( isInitialized.compareAndSet(false, true) )
                {
                    publishEvent(TreeCacheEvent.Type.INITIALIZED);
                }
            }
        }
    }

    private enum TreeState
    {
        LATENT,
        STARTED,
        CLOSED
    }

    /**
     * Tracks the number of outstanding background requests in flight. The first time this count reaches 0, we publish the initialized event.
     */
    private final AtomicLong outstandingOps = new AtomicLong(0);

    /**
     * Have we published the {@link TreeCacheEvent.Type#INITIALIZED} event yet?
     */
    private final AtomicBoolean isInitialized = new AtomicBoolean(false);

    private final TreeNode root;
    private final WatcherRemoveCuratorFramework client;
    private final ExecutorService executorService;
    private final boolean cacheData;
    private final boolean dataIsCompressed;
    private final int maxDepth;
    private final StandardListenerManager<TreeCacheListener> listeners = StandardListenerManager.standard();
    private final StandardListenerManager<UnhandledErrorListener> errorListeners = StandardListenerManager.standard();
    private final AtomicReference<TreeState> treeState = new AtomicReference<TreeState>(TreeState.LATENT);

    private final ConnectionStateListener connectionStateListener = new ConnectionStateListener()
    {
        @Override
        public void stateChanged(CuratorFramework client, ConnectionState newState)
        {
            handleStateChange(newState);
        }
    };

    static final ThreadFactory defaultThreadFactory = ThreadUtils.newThreadFactory("TreeCache");

    /**
     * Create a TreeCache for the given client and path with default options.
     * <p/>
     * If the client is namespaced, all operations on the resulting TreeCache will be in terms of
     * the namespace, including all published events.  The given path is the root at which the
     * TreeCache will watch and explore.  If no node exists at the given path, the TreeCache will
     * be initially empty.
     *
     * @param client the client to use; may be namespaced
     * @param path   the path to the root node to watch/explore; this path need not actually exist on
     *               the server
     * @see #newBuilder(CuratorFramework, String)
     */
    public TreeCache(CuratorFramework client, String path)
    {
        this(client, path, true, false, Integer.MAX_VALUE, Executors.newSingleThreadExecutor(defaultThreadFactory), false, false, new DefaultTreeCacheSelector());
    }

    /**
     * @param client           the client
     * @param path             path to watch
     * @param cacheData        if true, node contents are cached in addition to the stat
     * @param dataIsCompressed if true, data in the path is compressed
     * @param executorService  Closeable ExecutorService to use for the TreeCache's background thread
     * @param createParentNodes true to create parent nodes as containers
     * @param disableZkWatches true to disable Zookeeper watches
     * @param selector         the selector to use
     */
    TreeCache(CuratorFramework client, String path, boolean cacheData, boolean dataIsCompressed, int maxDepth, final ExecutorService executorService, boolean createParentNodes, boolean disableZkWatches, TreeCacheSelector selector)
    {
        this.createParentNodes = createParentNodes;
        this.selector = Preconditions.checkNotNull(selector, "selector cannot be null");
        this.root = new TreeNode(validatePath(path), null);
        Preconditions.checkNotNull(client, "client cannot be null");
        this.client = client.newWatcherRemoveCuratorFramework();
        this.cacheData = cacheData;
        this.dataIsCompressed = dataIsCompressed;
        this.maxDepth = maxDepth;
        this.disableZkWatches = disableZkWatches;
        this.executorService = Preconditions.checkNotNull(executorService, "executorService cannot be null");
    }

    /**
     * Start the cache. The cache is not started automatically. You must call this method.
     *
     * @return this
     * @throws Exception errors
     */
    public TreeCache start() throws Exception
    {
        Preconditions.checkState(treeState.compareAndSet(TreeState.LATENT, TreeState.STARTED), "already started");
        if ( createParentNodes )
        {
            client.createContainers(root.path);
        }
        client.getConnectionStateListenable().addListener(connectionStateListener);
        if ( client.getZookeeperClient().isConnected() )
        {
            root.wasCreated();
        }
        return this;
    }

    /**
     * Close/end the cache.
     */
    @Override
    public void close()
    {
        if ( treeState.compareAndSet(TreeState.STARTED, TreeState.CLOSED) )
        {
            client.removeWatchers();
            client.getConnectionStateListenable().removeListener(connectionStateListener);
            listeners.clear();
            executorService.shutdown();
            try
            {
                root.wasDeleted();
            }
            catch ( Exception e )
            {
                ThreadUtils.checkInterrupted(e);
                handleException(e);
            }
        }
    }

    /**
     * Return the cache listenable
     *
     * @return listenable
     */
    public Listenable<TreeCacheListener> getListenable()
    {
        return listeners;
    }

    /**
     * Allows catching unhandled errors in asynchronous operations.
     *
     * TODO: consider making public.
     */
    @VisibleForTesting
    public Listenable<UnhandledErrorListener> getUnhandledErrorListenable()
    {
        return errorListeners;
    }

    private TreeNode find(String findPath)
    {
        PathUtils.validatePath(findPath);
        LinkedList<String> rootElements = new LinkedList<String>(ZKPaths.split(root.path));
        LinkedList<String> findElements = new LinkedList<String>(ZKPaths.split(findPath));
        while (!rootElements.isEmpty()) {
            if (findElements.isEmpty()) {
                // Target path shorter than root path
                return null;
            }
            String nextRoot = rootElements.removeFirst();
            String nextFind = findElements.removeFirst();
            if (!nextFind.equals(nextRoot)) {
                // Initial root path does not match
                return null;
            }
        }

        TreeNode current = root;
        while (!findElements.isEmpty()) {
            String nextFind = findElements.removeFirst();
            ConcurrentMap<String, TreeNode> map = current.children;
            if ( map == null )
            {
                return null;
            }
            current = map.get(nextFind);
            if ( current == null )
            {
                return null;
            }
        }
        return current;
    }

    /**
     * Return the current set of children at the given path, mapped by child name. There are no
     * guarantees of accuracy; this is merely the most recent view of the data.  If there is no
     * node at this path, {@code null} is returned.
     *
     * @param fullPath full path to the node to check
     * @return a possibly-empty list of children if the node is alive, or null
     */
    public Map<String, ChildData> getCurrentChildren(String fullPath)
    {
        TreeNode node = find(fullPath);
        if ( node == null || !isLive(node.childData) )
        {
            return null;
        }
        ConcurrentMap<String, TreeNode> map = node.children;
        Map<String, ChildData> result;
        if ( map == null )
        {
            result = ImmutableMap.of();
        }
        else
        {
            ImmutableMap.Builder<String, ChildData> builder = ImmutableMap.builder();
            for ( Map.Entry<String, TreeNode> entry : map.entrySet() )
            {
                ChildData childData = entry.getValue().childData;
                // Double-check liveness after retrieving data.
                if ( isLive(childData) )
                {
                    builder.put(entry.getKey(), childData);
                }
            }
            result = builder.build();
        }

        // Double-check liveness after retrieving children.
        return isLive(node.childData) ? result : null;
    }

    /**
     * Return the current data for the given path. There are no guarantees of accuracy. This is
     * merely the most recent view of the data. If there is no node at the given path,
     * {@code null} is returned.
     *
     * @param fullPath full path to the node to check
     * @return data if the node is alive, or null
     */
    public ChildData getCurrentData(String fullPath)
    {
        TreeNode node = find(fullPath);
        if ( node == null )
        {
            return null;
        }
        ChildData result = node.childData;
        return isLive(result) ? result : null;
    }

    private void callListeners(final TreeCacheEvent event)
    {
        listeners.forEach(listener ->
        {
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

    /**
     * Send an exception to any listeners, or else log the error if there are none.
     */
    private void handleException(final Throwable e)
    {
        if ( errorListeners.size() == 0 )
        {
            LOG.error("", e);
        }
        else
        {
            errorListeners.forEach(listener ->
            {
                try
                {
                    listener.unhandledError("", e);
                }
                catch ( Exception e2 )
                {
                    ThreadUtils.checkInterrupted(e2);
                    LOG.error("Exception handling exception", e2);
                }
            });
        }
    }

    private void handleStateChange(ConnectionState newState)
    {
        switch ( newState )
        {
        case SUSPENDED:
            publishEvent(TreeCacheEvent.Type.CONNECTION_SUSPENDED);
            break;

        case LOST:
            isInitialized.set(false);
            publishEvent(TreeCacheEvent.Type.CONNECTION_LOST);
            break;

        case CONNECTED:
            try
            {
                root.wasCreated();
            }
            catch ( Exception e )
            {
                ThreadUtils.checkInterrupted(e);
                handleException(e);
            }
            break;

        case RECONNECTED:
            try
            {
                root.wasReconnected();
                publishEvent(TreeCacheEvent.Type.CONNECTION_RECONNECTED);
            }
            catch ( Exception e )
            {
                ThreadUtils.checkInterrupted(e);
                handleException(e);
            }
            break;
        }
    }

    private void publishEvent(TreeCacheEvent.Type type)
    {
        publishEvent(new TreeCacheEvent(type, null));
    }

    private void publishEvent(TreeCacheEvent.Type type, ChildData data)
    {
        publishEvent(new TreeCacheEvent(type, data));
    }

    private void publishEvent(final TreeCacheEvent event)
    {
        if ( treeState.get() != TreeState.CLOSED )
        {
            LOG.debug("publishEvent: {}", event);
            executorService.submit(new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        callListeners(event);
                    }
                    catch ( Exception e )
                    {
                        ThreadUtils.checkInterrupted(e);
                        handleException(e);
                    }
                }
            });
        }
    }
}
