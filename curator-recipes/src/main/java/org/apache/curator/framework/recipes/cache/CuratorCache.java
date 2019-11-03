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

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.listen.Listenable;
import java.io.Closeable;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * <p>
 *     A utility that attempts to keep the data from a node locally cached. Optionally the entire
 *     tree of children below the node can also be cached. Will respond to update/create/delete events, pull
 *     down the data, etc. You can register listeners that will get notified when changes occur.
 * </p>
 *
 * <p>
 *     <b>IMPORTANT</b> - Due to how ZooKeeper works you will not get notified of every single event.
 *     For example during a network partition the cache will not get events. Imagine the following scenario:
 *
 *     <ul>
 *         <li>Pre-network partition the cache contains "/foo" and "/foo/bar"</li>
 *         <li>A network partition occurs and your Curator client loses connection to the server</li>
 *         <li>Image another client that isn't partitioned, deletes "/foo/bar" and then a third client re-creates "/foo/bar"</li>
 *         <li>Your client's partition is fixed. The cache will only see <em>one</em> change - the third client's re-create</li>
 *     </ul>
 *
 *     Additionally, remember that ZooKeeper is an eventual consistent system. Always use ZNode version
 *     numbers when updating nodes.
 * </p>
 */
public interface CuratorCache extends Closeable, CuratorCacheAccessor
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
     * Return the listener container so that listeners can be registered to be notified of changes to the cache
     *
     * @return listener container
     */
    Listenable<CuratorCacheListener> listenable();

    /**
     * {@inheritDoc}
     */
    @Override
    Optional<ChildData> get(String path);

    /**
     * {@inheritDoc}
     */
    @Override
    int size();

    /**
     * {@inheritDoc}
     */
    @Override
    Stream<ChildData> stream();
}
