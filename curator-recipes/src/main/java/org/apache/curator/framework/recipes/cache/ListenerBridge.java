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

import com.google.common.base.Preconditions;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.recipes.watch.CacheEvent;
import org.apache.curator.framework.recipes.watch.CacheListener;
import org.apache.curator.framework.recipes.watch.CachedNode;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * <p>
 * Utility to bridge old TreeCache {@link org.apache.curator.framework.recipes.cache.TreeCacheListener}
 * instances with new {@link org.apache.curator.framework.recipes.watch.CacheListener} so that you can
 * use existing listeners without rewriting them.
 * </p>
 *
 * <p>
 * Create a ListenerBridge from your existing TreeCacheListener. You can then call {@link #add()}
 * to add the bridge listener to a CuratorCache.
 * </p>
 */
public class ListenerBridge implements CacheListener, ConnectionStateListener
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final CuratorFramework client;
    private final Listenable<CacheListener> listenable;
    private final TreeCacheListener listener;
    private final AtomicBoolean added = new AtomicBoolean(false);

    /**
     * Builder style constructor
     *
     * @param client the client
     * @param listenable CuratorCache listener container
     * @param listener the old TreeCacheListener
     * @return listener bridge
     */
    public static ListenerBridge wrap(CuratorFramework client, Listenable<CacheListener> listenable, TreeCacheListener listener)
    {
        return new ListenerBridge(client, listenable, listener);
    }

    /**
     * @param client the client
     * @param listenable CuratorCache listener container
     * @param listener the old TreeCacheListener
     */
    public ListenerBridge(CuratorFramework client, Listenable<CacheListener> listenable, TreeCacheListener listener)
    {
        this.client = Objects.requireNonNull(client, "client cannot be null");
        this.listenable = Objects.requireNonNull(listenable, "listenable cannot be null");
        this.listener = Objects.requireNonNull(listener, "listener cannot be null");
    }

    @Override
    public void process(CacheEvent event, String path, CachedNode affectedNode)
    {
        try
        {
            listener.childEvent(client, toEvent(event, path, affectedNode));
        }
        catch ( Exception e )
        {
            handleException(e);
        }
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState)
    {
        TreeCacheEvent.Type type = toType(newState);
        if ( type != null )
        {
            try
            {
                listener.childEvent(client, new TreeCacheEvent(type, null));
            }
            catch ( Exception e )
            {
                handleException(e);
            }
        }
    }

    /**
     * Add this listener to the listener container. Note: this method is not idempotent
     */
    public void add()
    {
        Preconditions.checkState(added.compareAndSet(false, true), "Already added");
        client.getConnectionStateListenable().addListener(this);
        listenable.addListener(this);
    }

    /**
     * Remove this listener from the listener container
     */
    public void remove()
    {
        if ( added.compareAndSet(true, false) )
        {
            client.getConnectionStateListenable().removeListener(this);
            listenable.removeListener(this);
        }
    }

    /**
     * Utility - convert a new CuratorCache event to an old TreeCache event
     *
     * @param event event to convert
     * @return new value
     */
    public static TreeCacheEvent.Type toType(CacheEvent event)
    {
        switch ( event )
        {
        case NODE_CREATED:
            return TreeCacheEvent.Type.NODE_ADDED;

        case NODE_DELETED:
            return TreeCacheEvent.Type.NODE_REMOVED;

        case NODE_CHANGED:
            return TreeCacheEvent.Type.NODE_UPDATED;

        case CACHE_REFRESHED:
            return TreeCacheEvent.Type.INITIALIZED;
        }

        throw new IllegalStateException("Unknown event: " + event);
    }

    /**
     * Utility - convert a connection state event to an old TreeCache event
     *
     * @param state event to convert
     * @return new value or null if there is no corresponding TreeCache value
     */
    public static TreeCacheEvent.Type toType(ConnectionState state)
    {
        switch ( state )
        {
        case RECONNECTED:
            return TreeCacheEvent.Type.CONNECTION_RECONNECTED;

        case SUSPENDED:
            return TreeCacheEvent.Type.CONNECTION_SUSPENDED;

        case LOST:
            return TreeCacheEvent.Type.CONNECTION_LOST;
        }

        return null;
    }

    /**
     * Convert Curator Cache listener values to TreeCache data
     *
     * @param path the affected path (can be null)
     * @param affectedNode the node (can be null)
     * @return TreeCache data or null
     */
    public static ChildData toData(String path, CachedNode affectedNode)
    {
        if ( (path != null) && (affectedNode != null) && (affectedNode.getData() != null) )
        {
            return new ChildData(path, affectedNode.getStat(), affectedNode.getData());
        }
        return null;
    }

    /**
     * Generate a TreeCacheEvent from Curator cache event data
     *
     * @param event event type
     * @param path affected path (can be null)
     * @param affectedNode affected data (can be null)
     * @return event
     */
    public static TreeCacheEvent toEvent(CacheEvent event, String path, CachedNode affectedNode)
    {
        TreeCacheEvent.Type type = toType(event);
        ChildData data = (event == CacheEvent.CACHE_REFRESHED) ? null : toData(path, affectedNode);
        return new TreeCacheEvent(type, data);
    }

    protected void handleException(Exception e)
    {
        log.error("Unhandled exception in listener", e);
    }
}
