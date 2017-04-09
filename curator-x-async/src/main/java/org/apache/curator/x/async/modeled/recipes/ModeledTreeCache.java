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
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.x.async.modeled.ModeledDetails;
import org.apache.curator.x.async.modeled.ZPath;
import org.apache.curator.x.async.modeled.details.recipes.ModeledTreeCacheImpl;
import java.io.Closeable;
import java.util.Map;
import java.util.Optional;

/**
 * Wraps a {@link org.apache.curator.framework.recipes.cache.TreeCache} so that
 * node data can be viewed as strongly typed models.
 */
public interface ModeledTreeCache<T> extends Closeable
{
    /**
     * Return a newly wrapped cache
     *
     * @param modeled modeling options
     * @param cache the cache to wrap
     * @return new wrapped cache
     */
    static <T> ModeledTreeCache<T> wrap(ModeledDetails<T> modeled, TreeCache cache)
    {
        return new ModeledTreeCacheImpl<>(modeled, cache);
    }

    /**
     * Return the original cache that was wrapped
     *
     * @return cache
     */
    TreeCache unwrap();

    /**
     * Forwards to {@link org.apache.curator.framework.recipes.cache.TreeCache#start()}
     */
    void start();

    /**
     * Forwards to {@link org.apache.curator.framework.recipes.cache.TreeCache#close()}
     */
    void close();

    /**
     * Return the listener container so that you can add/remove listeners
     *
     * @return listener container
     */
    Listenable<ModeledCacheListener<T>> getListenable();

    /**
     * Return the modeled current set of children at the given path, mapped by child name. There are no
     * guarantees of accuracy; this is merely the most recent view of the data.
     *
     * @param fullPath full path to the node to check
     * @return a possibly-empty list of children if the node is alive, or null
     */
    Map<ZPath, ModeledCachedNode<T>> getCurrentChildren(ZPath fullPath);

    /**
     * Return the modeled current data for the given path. There are no guarantees of accuracy. This is
     * merely the most recent view of the data. If there is no node at the given path,
     * {@link java.util.Optional#empty()} is returned.
     *
     * @param fullPath full path to the node to check
     * @return data if the node is alive, or null
     */
    Optional<ModeledCachedNode<T>> getCurrentData(ZPath fullPath);
}
