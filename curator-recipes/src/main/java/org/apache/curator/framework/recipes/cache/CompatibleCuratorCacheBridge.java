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

import static org.apache.curator.framework.recipes.cache.CuratorCacheListener.Type.*;
import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.listen.StandardListenerManager;

/**
 * Version of CuratorCacheBridge for pre-ZK 3.6 - uses TreeCache instead of CuratorCache
 */
@SuppressWarnings("deprecation")
class CompatibleCuratorCacheBridge implements CuratorCacheBridge, TreeCacheListener {
    private final TreeCache cache;
    private final StandardListenerManager<CuratorCacheListener> listenerManager = StandardListenerManager.standard();

    CompatibleCuratorCacheBridge(
            CuratorFramework client,
            String path,
            CuratorCache.Options[] optionsArg,
            ExecutorService executorService,
            boolean cacheData) {
        Set<CuratorCache.Options> options = (optionsArg != null) ? Sets.newHashSet(optionsArg) : Collections.emptySet();
        TreeCache.Builder builder = TreeCache.newBuilder(client, path).setCacheData(cacheData);
        if (options.contains(CuratorCache.Options.SINGLE_NODE_CACHE)) {
            builder.setMaxDepth(0);
        }
        if (options.contains(CuratorCache.Options.COMPRESSED_DATA)) {
            builder.setDataIsCompressed(true);
        }
        if (executorService != null) {
            builder.setExecutor(executorService);
        }
        cache = builder.build();
    }

    @Override
    public void start() {
        try {
            cache.getListenable().addListener(this);

            cache.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        cache.close();
    }

    @Override
    public boolean isCuratorCache() {
        return false;
    }

    @Override
    public Listenable<CuratorCacheListener> listenable() {
        return listenerManager;
    }

    @Override
    public Optional<ChildData> get(String path) {
        return Optional.ofNullable(cache.getCurrentData(path));
    }

    @Override
    public int size() {
        return cache.size();
    }

    @Override
    public Stream<ChildData> stream() {
        Iterable<ChildData> iterable = cache::iterator;
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    @Override
    public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
        switch (event.getType()) {
            case NODE_ADDED: {
                listenerManager.forEach(listener -> listener.event(NODE_CREATED, null, event.getData()));
                break;
            }

            case NODE_REMOVED: {
                listenerManager.forEach(listener -> listener.event(NODE_DELETED, event.getData(), null));
                break;
            }

            case NODE_UPDATED: {
                listenerManager.forEach(listener -> listener.event(NODE_CHANGED, event.getOldData(), event.getData()));
                break;
            }

            case INITIALIZED: {
                listenerManager.forEach(CuratorCacheListener::initialized);
                break;
            }
        }
    }
}
