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

package org.apache.curator.v2.recipes.cache;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.v2.CuratorFrameworkV2;
import java.io.Closeable;

/**
 * <p>
 *     A utility that attempts to keep the data from a node locally cached. Optionally the entire
 *     tree of children below the node can also be cached. Will respond to update/create/delete events, pull
 *     down the data, etc. You can register listeners that will get notified when changes occur.
 * </p>
 *
 * <p>
 *     <b>IMPORTANT</b> - it's not possible to stay transactionally in sync. Users of this class must
 *     be prepared for false-positives and false-negatives. Additionally, always use the version number
 *     when updating data to avoid overwriting another process' change.
 * </p>
 */
public interface CuratorCache extends Closeable
{
    /**
     * cache build options
     */
    enum Options
    {
        /**
         * Normally the entire tree of nodes starting at the given node are cached. This option
         * causes only the given node to be cached (i.e. a single node cache)
         */
        SINGLE_NODE_CACHE,

        /**
         * Decompress data via {@link org.apache.curator.framework.api.GetDataBuilder#decompressed()}
         */
        COMPRESSED_DATA,

        /**
         * Normally, when the cache is closed via {@link CuratorCache#close()}, the storage is cleared
         * via {@link CuratorCacheStorage#clear()}. This option prevents the storage from being cleared.
         */
        DO_NOT_CLEAR_ON_CLOSE
    }

    /**
     * Return a Curator Cache for the given path with the given options using a standard storage instance
     *
     * @param client Curator client
     * @param path path to cache
     * @param options any options
     * @return cache (note it must be started via {@link #start()}
     */
    static CuratorCache build(CuratorFramework client, String path, Options... options)
    {
        return builder(client, path).withOptions(options).build();
    }

    /**
     * Start a Curator Cache builder
     *
     * @param client Curator client
     * @param path path to cache
     * @return builder
     */
    static CuratorCacheBuilder builder(CuratorFramework client, String path)
    {
        return new CuratorCacheBuilderImpl(client, path);
    }

    /**
     * Return a Curator Cache for the given path with the given options using a standard storage instance
     *
     * @param client Curator client
     * @param path path to cache
     * @param options any options
     * @return cache (note it must be started via {@link #start()}
     */
    static CuratorCache build(CuratorFrameworkV2 client, String path, Options... options)
    {
        return builder(client, path).withOptions(options).build();
    }

    /**
     * Start a Curator Cache builder
     *
     * @param client Curator client
     * @param path path to cache
     * @return builder
     */
    static CuratorCacheBuilder builder(CuratorFrameworkV2 client, String path)
    {
        return new CuratorCacheBuilderImpl(client, path);
    }

    /**
     * Start the cache. This will cause a complete refresh from the cache's root node and generate
     * events for all nodes found, etc.
     */
    void start();

    /**
     * Close the cache, stop responding to events, etc.
     */
    @Override
    void close();

    /**
     * Return the storage instance being used
     *
     * @return storage
     */
    CuratorCacheStorage storage();

    /**
     * Return the listener container so that listeners can be registered to be notified of changes to the cache
     *
     * @return listener container
     */
    Listenable<CuratorCacheListener> listenable();
}
