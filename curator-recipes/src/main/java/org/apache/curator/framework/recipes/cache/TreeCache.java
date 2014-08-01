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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.listen.ListenerContainer;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.utils.CloseableExecutorService;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * <p>A utility that attempts to keep all data from all children of a ZK path locally cached. This class
 * will watch the ZK path, respond to update/create/delete events, pull down the data, etc. You can
 * register a listener that will get notified when changes occur.</p>
 * <p></p>
 * <p><b>IMPORTANT</b> - it's not possible to stay transactionally in sync. Users of this class must
 * be prepared for false-positives and false-negatives. Additionally, always use the version number
 * when updating data to avoid overwriting another process' change.</p>
 */
public class TreeCache implements Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(TreeCache.class);

    private enum NodeState
    {
        PENDING, LIVE, DEAD
    }

    private final class TreeNode implements Watcher, BackgroundCallback
    {
        final AtomicReference<NodeState> nodeState = new AtomicReference<NodeState>(NodeState.PENDING);
        final TreeNode parent;
        final String path;
        final AtomicReference<Stat> stat = new AtomicReference<Stat>();
        final AtomicReference<byte[]> data = new AtomicReference<byte[]>();
        final AtomicReference<ConcurrentMap<String, TreeNode>> children = new AtomicReference<ConcurrentMap<String, TreeNode>>();

        TreeNode(String path, TreeNode parent)
        {
            this.path = path;
            this.parent = parent;
        }

        private void refresh() throws Exception
        {
            outstandingOps.addAndGet(2);
            doRefreshData();
            doRefreshChildren();
        }

        private void refreshChildren() throws Exception
        {
            outstandingOps.incrementAndGet();
            doRefreshChildren();
        }

        private void refreshData() throws Exception
        {
            outstandingOps.incrementAndGet();
            doRefreshData();
        }

        private void doRefreshChildren() throws Exception
        {
            client.getChildren().usingWatcher(this).inBackground(this).forPath(path);
        }

        private void doRefreshData() throws Exception
        {
            if ( dataIsCompressed )
            {
                client.getData().decompressed().usingWatcher(this).inBackground(this).forPath(path);
            }
            else
            {
                client.getData().usingWatcher(this).inBackground(this).forPath(path);
            }
        }

        void wasReconnected() throws Exception
        {
            refresh();
            ConcurrentMap<String, TreeNode> childMap = children.get();
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
            stat.set(null);
            data.set(null);
            client.clearWatcherReferences(this);
            ConcurrentMap<String, TreeNode> childMap = children.getAndSet(null);
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

            if ( nodeState.compareAndSet(NodeState.LIVE, NodeState.DEAD) )
            {
                publishEvent(TreeCacheEvent.Type.NODE_REMOVED, path);
            }

            if ( parent == null )
            {
                // Root node; use an exist query to watch for existence.
                client.checkExists().usingWatcher(this).inBackground().forPath(path);
            }
            else
            {
                // Remove from parent if we're currently a child
                ConcurrentMap<String, TreeNode> parentChildMap = parent.children.get();
                if ( parentChildMap != null )
                {
                    parentChildMap.remove(ZKPaths.getNodeFromPath(path), this);
                }
            }
        }

        @Override
        public void process(WatchedEvent event)
        {
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
                handleException(e);
            }
        }

        @Override
        public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
        {
            Stat newStat = event.getStat();
            switch ( event.getType() )
            {
            case EXISTS:
                Preconditions.checkState(parent == null, "unexpected EXISTS on non-root node");
                if ( event.getResultCode() == KeeperException.Code.OK.intValue() )
                {
                    nodeState.compareAndSet(NodeState.DEAD, NodeState.PENDING);
                    wasCreated();
                }
                else if ( event.getResultCode() == KeeperException.Code.NONODE.intValue() )
                {
                    wasDeleted();
                }
                break;
            case CHILDREN:
                if ( event.getResultCode() == KeeperException.Code.OK.intValue() )
                {
                    Stat oldStat = stat.get();
                    if (oldStat != null && oldStat.getMzxid() == newStat.getMzxid()) {
                        // Only update stat if mzxid is different, otherwise we might obscure
                        // GET_DATA event updates.
                        stat.set(newStat);
                    }

                    if ( event.getChildren().isEmpty() )
                    {
                        break;
                    }

                    ConcurrentMap<String, TreeNode> childMap = children.get();
                    if ( childMap == null )
                    {
                        childMap = Maps.newConcurrentMap();
                        if ( !children.compareAndSet(null, childMap) )
                        {
                            childMap = children.get();
                        }
                    }

                    // Present new children in sorted order for test determinism.
                    List<String> newChildren = new ArrayList<String>();
                    for ( String child : event.getChildren() )
                    {
                        if ( !childMap.containsKey(child) )
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
                    if ( cacheData )
                    {
                        data.set(event.getData());
                    }

                    Stat oldStat = stat.getAndSet(newStat);
                    if ( nodeState.compareAndSet(NodeState.PENDING, NodeState.LIVE) )
                    {
                        publishEvent(TreeCacheEvent.Type.NODE_ADDED, new ChildData(event.getPath(), newStat, event.getData()));
                    }
                    else
                    {
                        if ( oldStat == null || oldStat.getMzxid() != newStat.getMzxid() )
                        {
                            publishEvent(TreeCacheEvent.Type.NODE_UPDATED, new ChildData(event.getPath(), newStat, event.getData()));
                        }
                    }
                }
                else if ( event.getResultCode() == KeeperException.Code.NONODE.intValue() )
                {
                    wasDeleted();
                }
                break;
            default:
                handleException(new Exception(String.format("Unknown event %s", event)));
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
    private final CuratorFramework client;
    private final CloseableExecutorService executorService;
    private final boolean cacheData;
    private final boolean dataIsCompressed;
    private final ListenerContainer<TreeCacheListener> listeners = new ListenerContainer<TreeCacheListener>();
    private final AtomicReference<TreeState> treeState = new AtomicReference<TreeState>(TreeState.LATENT);

    private final ConnectionStateListener connectionStateListener = new ConnectionStateListener()
    {
        @Override
        public void stateChanged(CuratorFramework client, ConnectionState newState)
        {
            handleStateChange(newState);
        }
    };

    private static final ThreadFactory defaultThreadFactory = ThreadUtils.newThreadFactory("TreeCache");

    /**
     * @param client    the client
     * @param path      path to watch
     * @param cacheData if true, node contents are cached in addition to the stat
     */
    public TreeCache(CuratorFramework client, String path, boolean cacheData)
    {
        this(client, path, cacheData, false, new CloseableExecutorService(Executors.newSingleThreadExecutor(defaultThreadFactory), true));
    }

    /**
     * @param client        the client
     * @param path          path to watch
     * @param cacheData     if true, node contents are cached in addition to the stat
     * @param threadFactory factory to use when creating internal threads
     */
    public TreeCache(CuratorFramework client, String path, boolean cacheData, ThreadFactory threadFactory)
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
    public TreeCache(CuratorFramework client, String path, boolean cacheData, boolean dataIsCompressed, ThreadFactory threadFactory)
    {
        this(client, path, cacheData, dataIsCompressed, new CloseableExecutorService(Executors.newSingleThreadExecutor(threadFactory), true));
    }

    /**
     * @param client           the client
     * @param path             path to watch
     * @param cacheData        if true, node contents are cached in addition to the stat
     * @param dataIsCompressed if true, data in the path is compressed
     * @param executorService  ExecutorService to use for the TreeCache's background thread
     */
    public TreeCache(CuratorFramework client, String path, boolean cacheData, boolean dataIsCompressed, final ExecutorService executorService)
    {
        this(client, path, cacheData, dataIsCompressed, new CloseableExecutorService(executorService));
    }

    /**
     * @param client           the client
     * @param path             path to watch
     * @param cacheData        if true, node contents are cached in addition to the stat
     * @param dataIsCompressed if true, data in the path is compressed
     * @param executorService  Closeable ExecutorService to use for the TreeCache's background thread
     */
    public TreeCache(CuratorFramework client, String path, boolean cacheData, boolean dataIsCompressed, final CloseableExecutorService executorService)
    {
        this.root = new TreeNode(path, null);
        this.client = client;
        this.cacheData = cacheData;
        this.dataIsCompressed = dataIsCompressed;
        this.executorService = executorService;
    }

    /**
     * Start the cache. The cache is not started automatically. You must call this method.
     *
     * @throws Exception errors
     */
    public void start() throws Exception
    {
        Preconditions.checkState(treeState.compareAndSet(TreeState.LATENT, TreeState.STARTED), "already started");
        client.getConnectionStateListenable().addListener(connectionStateListener);
        root.wasCreated();
    }

    /**
     * Close/end the cache.
     */
    @Override
    public void close()
    {
        if ( treeState.compareAndSet(TreeState.STARTED, TreeState.CLOSED) )
        {
            client.getConnectionStateListenable().removeListener(connectionStateListener);
            listeners.clear();
            executorService.close();
            try
            {
                root.wasDeleted();
            }
            catch ( Exception e )
            {
                handleException(e);
            }
        }
    }

    /**
     * Return the cache listenable
     *
     * @return listenable
     */
    public ListenerContainer<TreeCacheListener> getListenable()
    {
        return listeners;
    }

    private TreeNode find(String fullPath)
    {
        if ( !fullPath.startsWith(root.path) )
        {
            return null;
        }

        TreeNode current = root;
        if ( fullPath.length() > root.path.length() )
        {
            if ( root.path.length() > 1 )
            {
                fullPath = fullPath.substring(root.path.length());
            }
            List<String> split = ZKPaths.split(fullPath);
            for ( String part : split )
            {
                ConcurrentMap<String, TreeNode> map = current.children.get();
                if ( map == null )
                {
                    return null;
                }
                current = map.get(part);
                if ( current == null )
                {
                    return null;
                }
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
        if ( node == null || node.nodeState.get() != NodeState.LIVE )
        {
            return null;
        }
        ConcurrentMap<String, TreeNode> map = node.children.get();
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
                TreeNode childNode = entry.getValue();
                ChildData childData = new ChildData(childNode.path, childNode.stat.get(), childNode.data.get());
                // Double-check liveness after retreiving data.
                if ( childNode.nodeState.get() == NodeState.LIVE )
                {
                    builder.put(entry.getKey(), childData);
                }
            }
            result = builder.build();
        }

        // Double-check liveness after retreiving children.
        return node.nodeState.get() == NodeState.LIVE ? result : null;
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
        if ( node == null || node.nodeState.get() != NodeState.LIVE )
        {
            return null;
        }
        ChildData result = new ChildData(node.path, node.stat.get(), node.data.get());
        // Double-check liveness after retreiving data.
        return node.nodeState.get() == NodeState.LIVE ? result : null;
    }

    private void callListeners(final TreeCacheEvent event)
    {
        listeners.forEach(new Function<TreeCacheListener, Void>()
        {
            @Override
            public Void apply(TreeCacheListener listener)
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
        });
    }

    /**
     * Default behavior is just to log the exception
     *
     * @param e the exception
     */
    protected void handleException(Throwable e)
    {
        LOG.error("", e);
    }

    private void handleStateChange(ConnectionState newState)
    {
        switch ( newState )
        {
        case SUSPENDED:
            publishEvent(TreeCacheEvent.Type.CONNECTION_SUSPENDED);
            break;

        case LOST:
            publishEvent(TreeCacheEvent.Type.CONNECTION_LOST);
            break;

        case RECONNECTED:
            try
            {
                root.wasReconnected();
                publishEvent(TreeCacheEvent.Type.CONNECTION_RECONNECTED);
            }
            catch ( Exception e )
            {
                handleException(e);
            }
            break;
        }
    }

    private void publishEvent(TreeCacheEvent.Type type)
    {
        publishEvent(new TreeCacheEvent(type, null));
    }

    private void publishEvent(TreeCacheEvent.Type type, String path)
    {
        publishEvent(new TreeCacheEvent(type, new ChildData(path, null, null)));
    }

    private void publishEvent(TreeCacheEvent.Type type, ChildData data)
    {
        publishEvent(new TreeCacheEvent(type, data));
    }

    private void publishEvent(final TreeCacheEvent event)
    {
        if ( treeState.get() != TreeState.CLOSED )
        {
            executorService.submit(new Runnable()
            {
                @Override
                public void run()
                {
                    {
                        try
                        {
                            callListeners(event);
                        }
                        catch ( Exception e )
                        {
                            handleException(e);
                        }
                    }
                }
            });
        }
    }
}
