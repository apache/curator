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
import org.apache.curator.framework.recipes.cache.CuratorCacheListener.Type;
import java.util.function.Consumer;

public interface CuratorCacheListenerBuilder
{
    /**
     * Add a standard listener
     *
     * @param listener listener to add
     * @return this
     */
    CuratorCacheListenerBuilder forAll(CuratorCacheListener listener);

    /**
     * Add a listener only for {@link Type#NODE_CREATED}
     *
     * @param listener listener to add
     * @return this
     */
    CuratorCacheListenerBuilder forCreates(Consumer<ChildData> listener);

    @FunctionalInterface
    interface ChangeListener
    {
        void event(ChildData oldNode, ChildData node);
    }

    /**
     * Add a listener only for {@link Type#NODE_CHANGED}
     *
     * @param listener listener to add
     * @return this
     */
    CuratorCacheListenerBuilder forChanges(ChangeListener listener);

    /**
     * Add a listener only both {@link Type#NODE_CREATED} and {@link Type#NODE_CHANGED}
     *
     * @param listener listener to add
     * @return this
     */
    CuratorCacheListenerBuilder forCreatesAndChanges(ChangeListener listener);

    /**
     * Add a listener only for {@link Type#NODE_DELETED}
     *
     * @param listener listener to add
     * @return this
     */
    CuratorCacheListenerBuilder forDeletes(Consumer<ChildData> listener);

    /**
     * Add a listener only for {@link CuratorCacheListener#initialized()}
     *
     * @param listener listener to add
     * @return this
     */
    CuratorCacheListenerBuilder forInitialized(Runnable listener);

    /**
     * Bridge listener. You can reuse old-style {@link org.apache.curator.framework.recipes.cache.PathChildrenCacheListener}s
     * with CuratorCache. IMPORTANT: the connection state methods in the listener will never be called as CuratorCache
     * does not register the listener with the connection state listener container. Also note that CuratorCache
     * behaves differently than {@link org.apache.curator.framework.recipes.cache.PathChildrenCache} so
     * things such as event ordering will likely be different.
     *
     * @param client the curator client
     * @param listener the listener to wrap
     * @return a CuratorCacheListener that forwards to the given listener
     */
    CuratorCacheListenerBuilder forPathChildrenCache(CuratorFramework client, PathChildrenCacheListener listener);

    /**
     * Bridge listener. You can reuse old-style {@link org.apache.curator.framework.recipes.cache.TreeCacheListener}s
     * with CuratorCache. IMPORTANT: the connection state methods in the listener will never be called as CuratorCache
     * does not register the listener with the connection state listener container. Also note that CuratorCache
     * behaves differently than {@link org.apache.curator.framework.recipes.cache.TreeCache} so
     * things such as event ordering will likely be different.
     *
     * @param client the curator client
     * @param listener the listener to wrap
     * @return a CuratorCacheListener that forwards to the given listener
     */
    CuratorCacheListenerBuilder forTreeCache(CuratorFramework client, TreeCacheListener listener);

    /**
     * Bridge listener. You can reuse old-style {@link org.apache.curator.framework.recipes.cache.NodeCacheListener}s
     * with CuratorCache.
     *
     * @param listener the listener to wrap
     * @return a CuratorCacheListener that forwards to the given listener
     */
    CuratorCacheListenerBuilder forNodeCache(NodeCacheListener listener);

    /**
     * Make the built listener so that it only becomes active once {@link CuratorCacheListener#initialized()} has been called.
     * i.e. changes that occur as the cache is initializing are not sent to the listener
     */
    CuratorCacheListenerBuilder afterInitialized();

    /**
     * Build and return a new listener based on the methods that have been previously called
     *
     * @return new listener
     */
    CuratorCacheListener build();
}