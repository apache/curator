/*
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

package org.apache.curator.x.async.modeled.cached;

import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.x.async.AsyncStage;
import org.apache.curator.x.async.modeled.ModeledFramework;
import org.apache.curator.x.async.modeled.ZNode;
import org.apache.curator.x.async.modeled.ZPath;
import org.apache.zookeeper.data.Stat;
import java.io.Closeable;
import java.util.List;

public interface CachedModeledFramework<T> extends ModeledFramework<T>, Closeable
{
    /**
     * Return the cache instance
     *
     * @return cache
     */
    ModeledCache<T> cache();

    /**
     * Start the internally created cache
     */
    void start();

    /**
     * Close/stop the internally created cache
     */
    @Override
    void close();

    /**
     * Return the listener container so that you can add/remove listeners
     *
     * @return listener container
     */
    Listenable<ModeledCacheListener<T>> listenable();

    /**
     * Same as {@link org.apache.curator.x.async.modeled.ModeledFramework#childrenAsZNodes()}
     * but always reads from cache - i.e. no additional queries to ZooKeeper are made
     *
     * @return AsyncStage stage
     */
    @Override
    AsyncStage<List<ZNode<T>>> childrenAsZNodes();

    /**
     * {@inheritDoc}
     */
    @Override
    CachedModeledFramework<T> child(Object child);

    /**
     * {@inheritDoc}
     */
    @Override
    CachedModeledFramework<T> withPath(ZPath path);

    /**
     * Same as {@link #read()} except that if the cache does not have a value
     * for this path a direct query is made.
     *
     * @return AsyncStage
     * @see org.apache.curator.x.async.AsyncStage
     */
    AsyncStage<T> readThrough();

    /**
     * Same as {@link #read(org.apache.zookeeper.data.Stat)} except that if the cache does not have a value
     * for this path a direct query is made.
     *
     * @param storingStatIn the stat for the new ZNode is stored here
     * @return AsyncStage
     * @see org.apache.curator.x.async.AsyncStage
     */
    AsyncStage<T> readThrough(Stat storingStatIn);

    /**
     * Same as {@link #readAsZNode()} except that if the cache does not have a value
     * for this path a direct query is made.
     *
     *
     * @return AsyncStage
     * @see org.apache.curator.x.async.AsyncStage
     */
    AsyncStage<ZNode<T>> readThroughAsZNode();

    /**
     * Return the instances of the base path of this cached framework
     *
     * @return listing of all models in the base path
     */
    AsyncStage<List<T>> list();
}
