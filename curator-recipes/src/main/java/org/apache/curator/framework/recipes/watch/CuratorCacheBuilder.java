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

import com.google.common.base.Preconditions;
import org.apache.curator.framework.CuratorFramework;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Factory for building cache instances
 */
public class CuratorCacheBuilder
{
    private final CuratorFramework client;
    private final String path;
    private CacheAction singleNodeCacheAction = null;
    private boolean sendRefreshEvents = true;
    private boolean refreshOnStart = true;
    private boolean sortChildren = true;
    private CachedNodeComparator nodeComparator = CachedNodeComparators.dataAndType();
    private CacheSelector cacheSelector = CacheSelectors.statAndData();
    private CachedNodeMapFactory cachedNodeMapFactory = GuavaCachedNodeMap.factory;
    private boolean usingWeakValues = false;
    private boolean usingSoftValues = false;
    private Long expiresAfterWriteMs = null;
    private Long expiresAfterAccessMs = null;

    /**
     * Start a new builder for the given client and main path
     *
     * @param client the client
     * @param path the base path for the cache
     * @return builder
     */
    public static CuratorCacheBuilder builder(CuratorFramework client, String path)
    {
        return new CuratorCacheBuilder(client, path);
    }

    /**
     * Return a new cache based on the current values of the builder. Note: the cache must
     * be started before use and closed when done.
     *
     * @return cache instance
     */
    public CuratorCache build()
    {
        CachedNodeMap cachedNodeMap = cachedNodeMapFactory.create(usingWeakValues, usingSoftValues, expiresAfterWriteMs, expiresAfterAccessMs);
        if ( singleNodeCacheAction != null )
        {
            Preconditions.checkState(cacheSelector == null, "Single node mode does not support CacheSelectors");
            return new InternalNodeCache(client, path, singleNodeCacheAction, nodeComparator, cachedNodeMap, sendRefreshEvents, refreshOnStart);
        }

        return new InternalCuratorCache(client, path, cacheSelector, nodeComparator, cachedNodeMap, sendRefreshEvents, refreshOnStart, sortChildren);
    }

    /**
     * Single node caches can be optimized. Call this method to only watch/cache a single node
     *
     * @return this
     */
    public CuratorCacheBuilder forSingleNode()
    {
        return forSingleNode(CacheAction.STAT_AND_DATA);
    }

    /**
     * Single node caches can be optimized. Call this method to only watch/cache a single node
     *
     * @param cacheAction caching action to use
     * @return this
     */
    public CuratorCacheBuilder forSingleNode(CacheAction cacheAction)
    {
        cacheSelector = null;
        singleNodeCacheAction = Objects.requireNonNull(cacheAction, "cacheAction cannot be null");
        return this;
    }

    /**
     * By default, values in the cache use hard references. Calling this method changes to weak
     * references.
     *
     * @return this
     */
    public CuratorCacheBuilder usingWeakValues()
    {
        usingWeakValues = true;
        return this;
    }

    /**
     * By default, values in the cache use hard references. Calling this method changes to soft
     * references.
     *
     * @return this
     */
    public CuratorCacheBuilder usingSoftValues()
    {
        usingSoftValues = true;
        return this;
    }

    /**
     * By default, values in the cache never expire. Calling this method specifies that each entry
     * should be automatically removed from the cache once a fixed duration has elapsed after
     * the entry's creation, or the most recent replacement of its value.
     *
     * @param duration the length of time after an entry is created that it should be automatically
     *     removed
     * @param unit the unit that {@code duration} is expressed in
     * @return this
     */
    public CuratorCacheBuilder thatExpiresAfterWrite(long duration, TimeUnit unit)
    {
        expiresAfterWriteMs = unit.toMillis(duration);
        return this;
    }

    /**
     * By default, values in the cache never expire. Calling this method specifies that each entry
     * should be automatically removed from the cache once a fixed duration has elapsed after the
     * entry's creation, the most recent replacement of its value, or its last access.
     *
     * @param duration the length of time after an entry is last accessed that it should be
     *     automatically removed
     * @param unit the unit that {@code duration} is expressed in
     * @return this
     */
    public CuratorCacheBuilder thatExpiresAfterAccess(long duration, TimeUnit unit)
    {
        expiresAfterAccessMs = unit.toMillis(duration);
        return this;
    }

    /**
     * By default, the internal {@link org.apache.curator.framework.recipes.watch.CachedNodeMap} is
     * an instance of {@link org.apache.curator.framework.recipes.watch.GuavaCachedNodeMap}.
     * Use this method to change the factory that creates CachedNodeMap instances.
     *
     * @param cachedNodeMapFactory new factory
     * @return this
     */
    public CuratorCacheBuilder usingCachedNodeMapFactory(CachedNodeMapFactory cachedNodeMapFactory)
    {
        this.cachedNodeMapFactory = Objects.requireNonNull(cachedNodeMapFactory, "cachedNodeMapFactory cannot be null");
        return this;
    }

    /**
     * Changes whether or not {@link CacheEvent#CACHE_REFRESHED} events are sent to
     * {@link CacheListener}s. The default is <code>true</code>
     *
     * @param sendRefreshEvents true/false
     * @return this
     */
    public CuratorCacheBuilder sendingRefreshEvents(boolean sendRefreshEvents)
    {
        this.sendRefreshEvents = sendRefreshEvents;
        return this;
    }

    /**
     * Changes whether or not a refresh is done when the cache is started.  The default is <code>true</code>
     *
     * @param refreshOnStart true/false
     * @return this
     */
    public CuratorCacheBuilder refreshingWhenStarted(boolean refreshOnStart)
    {
        this.refreshOnStart = refreshOnStart;
        return this;
    }

    /**
     * Changes whether or not children are sorted when notifying. i.e. notifications of added nodes
     * come in sorted order. The default is <code>true</code>
     *
     * @param sortChildren true/false
     * @return this
     */
    public CuratorCacheBuilder sortingChildren(boolean sortChildren)
    {
        this.sortChildren = sortChildren;
        return this;
    }

    /**
     * Changes which comparator is used to determine whether a node has changed or not.
     * The default is {@link CachedNodeComparators#dataAndType}
     *
     * @param nodeComparator new comparator
     * @return this
     */
    public CuratorCacheBuilder withNodeComparator(CachedNodeComparator nodeComparator)
    {
        this.nodeComparator = Objects.requireNonNull(nodeComparator, "nodeComparator cannot be null");
        return this;
    }

    /**
     * Changes which cache selector is used. The default is {@link CacheSelectors#statAndData}
     *
     * @param cacheSelector new selector
     * @return this
     */
    public CuratorCacheBuilder withCacheSelector(CacheSelector cacheSelector)
    {
        this.cacheSelector = Objects.requireNonNull(cacheSelector, "cacheSelector cannot be null");
        return this;
    }

    private CuratorCacheBuilder(CuratorFramework client, String path)
    {
        this.client = Objects.requireNonNull(client, "client cannot be null");
        this.path = Objects.requireNonNull(path, "path cannot be null");
    }
}
