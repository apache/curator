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
package org.apache.curator.x.async.modeled.recipes;

import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.x.async.modeled.ModelSerializer;
import org.apache.curator.x.async.modeled.ZPath;
import org.apache.curator.x.async.modeled.details.recipes.ModeledPathChildrenCacheImpl;
import java.io.Closeable;
import java.util.List;
import java.util.Optional;

/**
 * Wraps a {@link org.apache.curator.framework.recipes.cache.PathChildrenCache} so that
 * node data can be viewed as strongly typed models.
 */
public interface ModeledPathChildrenCache<T> extends ModeledCache<T>, Closeable
{
    /**
     * Return a newly wrapped cache
     *
     * @param cache the cache to wrap
     * @param serializer model serializer
     * @return new wrapped cache
     */
    static <T> ModeledPathChildrenCache<T> wrap(PathChildrenCache cache, ModelSerializer<T> serializer)
    {
        return new ModeledPathChildrenCacheImpl<>(cache, serializer);
    }

    /**
     * Return the original cache that was wrapped
     *
     * @return cache
     */
    PathChildrenCache unwrap();

    /**
     * Forwards to {@link org.apache.curator.framework.recipes.cache.PathChildrenCache#start()}
     */
    void start();

    /**
     * Forwards to {@link org.apache.curator.framework.recipes.cache.PathChildrenCache#start(org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode)}
     */
    void start(PathChildrenCache.StartMode mode);

    /**
     * Forwards to {@link org.apache.curator.framework.recipes.cache.PathChildrenCache#rebuild()}
     */
    void rebuild();

    /**
     * Forwards to {@link org.apache.curator.framework.recipes.cache.PathChildrenCache#rebuildNode(String)}
     */
    void rebuildNode(ZPath fullPath);

    /**
     * Return the listener container so that you can add/remove listeners
     *
     * @return listener container
     */
    Listenable<ModeledCacheListener<T>> getListenable();

    /**
     * Return the modeled current data. There are no guarantees of accuracy. This is
     * merely the most recent view of the data. The data is returned in sorted order.
     *
     * @return list of children and data
     */
    List<ModeledCachedNode<T>> getCurrentData();

    /**
     * Forwards to {@link org.apache.curator.framework.recipes.cache.PathChildrenCache#clearDataBytes(String)}
     */
    void clearDataBytes(ZPath fullPath);

    /**
     * Forwards to {@link org.apache.curator.framework.recipes.cache.PathChildrenCache#clearDataBytes(String, int)}
     */
    boolean clearDataBytes(ZPath fullPath, int ifVersion);

    /**
     * Forwards to {@link org.apache.curator.framework.recipes.cache.PathChildrenCache#clearAndRefresh()}
     */
    void clearAndRefresh();

    /**
     * Forwards to {@link org.apache.curator.framework.recipes.cache.PathChildrenCache#clear()}
     */
    void clear();

    /**
     * Forwards to {@link org.apache.curator.framework.recipes.cache.PathChildrenCache#rebuildNode(String)}
     */
    void close();
}
