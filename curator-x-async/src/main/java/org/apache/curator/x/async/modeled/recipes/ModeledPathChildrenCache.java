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

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.utils.CloseableExecutorService;
import org.apache.curator.x.async.modeled.ModeledDetails;
import org.apache.curator.x.async.modeled.details.recipes.ModeledPathChildrenCacheImpl;
import java.io.Closeable;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;

public interface ModeledPathChildrenCache<T> extends Closeable
{
    static <T> ModeledPathChildrenCache<T> build(CuratorFramework client, ModeledDetails<T> modeled)
    {
        return new ModeledPathChildrenCacheImpl<>(client, modeled, true, null, null, null);
    }

    static <T> ModeledPathChildrenCache<T> build(CuratorFramework client, ModeledDetails<T> modeled, boolean cacheData)
    {
        return new ModeledPathChildrenCacheImpl<>(client, modeled, cacheData, null, null, null);
    }

    static <T> ModeledPathChildrenCache<T> build(CuratorFramework client, ModeledDetails<T> modeled, boolean cacheData, ThreadFactory threadFactory)
    {
        return new ModeledPathChildrenCacheImpl<>(client, modeled, cacheData, threadFactory, null, null);
    }

    static <T> ModeledPathChildrenCache<T> build(CuratorFramework client, ModeledDetails<T> modeled, boolean cacheData, ExecutorService executorService)
    {
        return new ModeledPathChildrenCacheImpl<>(client, modeled, cacheData, null, executorService, null);
    }

    static <T> ModeledPathChildrenCache<T> build(CuratorFramework client, ModeledDetails<T> modeled, boolean cacheData, CloseableExecutorService executorService)
    {
        return new ModeledPathChildrenCacheImpl<>(client, modeled, cacheData, null, null, executorService);
    }

    void start();

    void start(PathChildrenCache.StartMode mode);

    void rebuild();

    void rebuildNode(String fullPath);

    Listenable<ModeledPathChildrenCacheListener> getListenable();

    List<ModeledCachedNode> getCurrentData();

    ModeledCachedNode getCurrentData(String fullPath);

    void clearDataBytes(String fullPath);

    boolean clearDataBytes(String fullPath, int ifVersion);

    void clearAndRefresh();

    void clear();

    void close();
}
