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
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.curator.utils.ZKPaths;

/**
 * Methods to access the underlying storage
 */
public interface CuratorCacheAccessor {
    /**
     * Return an entry from storage
     *
     * @param path path to get
     * @return entry or {@code empty()}
     */
    Optional<ChildData> get(String path);

    /**
     * Return the current number of entries in storage
     *
     * @return number of entries
     */
    int size();

    /**
     * Return a stream over the storage entries. Note: for a standard storage instance, the stream
     * behaves like a stream returned by {@link java.util.concurrent.ConcurrentHashMap#entrySet()}
     *
     * @return stream over entries
     */
    Stream<ChildData> stream();

    /**
     * Filter for a ChildData stream. Only ChildDatas with the given parent path
     * pass the filter. This is useful to stream one level below a given path in the cache.
     * e.g. to stream only the first level below the root of the cache.
     *
     * <code><pre>
     * CuratorCache cache = ...
     * cache.stream().filter(parentPathFilter(root))... // etc
     * </pre></code>
     *
     * @param parentPath the parent path to filter on
     * @return filtered stream
     */
    static Predicate<ChildData> parentPathFilter(String parentPath) {
        return d -> {
            ZKPaths.PathAndNode pathAndNode = ZKPaths.getPathAndNode(d.getPath());
            return pathAndNode.getPath().equals(parentPath);
        };
    }
}
