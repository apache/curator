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

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

/**
 * Storage implementation optimized for a {@link CuratorCache} created with the
 * {@link CuratorCache.Options#SINGLE_NODE_CACHE} option. In that mode only a
 * single node is cached, so a single {@link AtomicReference} is sufficient
 * instead of a {@link java.util.concurrent.ConcurrentHashMap}.
 */
class SingleNodeCuratorCacheStorage implements CuratorCacheStorage {
    private final AtomicReference<ChildData> data = new AtomicReference<>(null);
    private final boolean cacheBytes;

    SingleNodeCuratorCacheStorage(boolean cacheBytes) {
        this.cacheBytes = cacheBytes;
    }

    @Override
    public Optional<ChildData> put(ChildData childData) {
        ChildData localData = cacheBytes ? childData : new ChildData(childData.getPath(), childData.getStat(), null);
        return Optional.ofNullable(data.getAndSet(localData));
    }

    @Override
    public Optional<ChildData> remove(String path) {
        return Optional.ofNullable(data.getAndUpdate(current -> current != null && current.getPath().equals(path) ? null : current));
    }

    @Override
    public Optional<ChildData> get(String path) {
        ChildData childData = data.get();
        return (childData != null && childData.getPath().equals(path)) ? Optional.of(childData) : Optional.empty();
    }

    @Override
    public int size() {
        return (data.get() != null) ? 1 : 0;
    }

    @Override
    public Stream<ChildData> stream() {
        ChildData childData = data.get();
        return (childData != null) ? Stream.of(childData) : Stream.empty();
    }

    @Override
    public void clear() {
        data.set(null);
    }
}
