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

import java.util.Optional;
import java.util.stream.Stream;

/**
 * Interface for maintaining data in a {@link CuratorCache}
 */
public interface CuratorCacheStorage extends CuratorCacheAccessor
{
    /**
     * Return a new standard storage instance
     *
     * @return storage instance
     */
    static CuratorCacheStorage standard()
    {
        return new StandardCuratorCacheStorage(true);
    }

    /**
     * Return a new storage instance that does not retain the data bytes. i.e. ChildData objects
     * returned by this storage will always return {@code null} for {@link ChildData#getData()}.
     *
     * @return storage instance that does not retain data bytes
     */
    static CuratorCacheStorage bytesNotCached()
    {
        return new StandardCuratorCacheStorage(false);
    }

    /**
     * Add an entry to storage and return any previous entry at that path
     *
     * @param data entry to add
     * @return previous entry or {@code empty()}
     */
    Optional<ChildData> put(ChildData data);

    /**
     * Remove the entry from storage and return any previous entry at that path
     *
     * @param path path to remove
     * @return previous entry or {@code empty()}
     */
    Optional<ChildData> remove(String path);

    /**
     * Reset the storage to zero entries
     */
    void clear();

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
