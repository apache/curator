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

package org.apache.curator.framework.recipes.cache;

import java.util.concurrent.ExecutorService;

public interface CuratorCacheBridgeBuilder {
    /**
     * @param options any options
     * @return this
     */
    CuratorCacheBridgeBuilder withOptions(CuratorCache.Options... options);

    /**
     * The bridge cache will not retain the data bytes. i.e. ChildData objects
     * returned by the cache will always return {@code null} for {@link ChildData#getData()}
     *
     * @return this
     */
    CuratorCacheBridgeBuilder withDataNotCached();

    /**
     * If the old {@link org.apache.curator.framework.recipes.cache.TreeCache} is used by the bridge
     * (i.e. you are using ZooKeeper 3.5.x) then this executor service is passed to {@link org.apache.curator.framework.recipes.cache.TreeCache.Builder#setExecutor(java.util.concurrent.ExecutorService)}.
     * For {@link org.apache.curator.framework.recipes.cache.CuratorCache} this is not used and will be ignored (a warning will be logged).
     *
     * @param executorService executor to use for ZooKeeper 3.5.x
     * @return this
     */
    @SuppressWarnings("deprecation")
    CuratorCacheBridgeBuilder withExecutorService(ExecutorService executorService);

    /**
     * Return a new Curator Cache Bridge based on the builder methods that have been called
     *
     * @return new Curator Cache Bridge
     */
    CuratorCacheBridge build();
}
