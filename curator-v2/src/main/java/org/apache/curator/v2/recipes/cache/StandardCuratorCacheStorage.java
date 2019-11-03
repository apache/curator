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

import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.utils.ZKPaths;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

class StandardCuratorCacheStorage implements CuratorCacheStorage
{
    private final Map<String, ChildData> dataMap;
    private final boolean cacheBytes;

    StandardCuratorCacheStorage(boolean cacheBytes)
    {
        this.dataMap = new ConcurrentHashMap<>();
        this.cacheBytes = cacheBytes;
    }

    @Override
    public Optional<ChildData> put(ChildData data)
    {
        ChildData localData = cacheBytes ? data : new ChildData(data.getPath(), data.getStat(), null);
        return Optional.ofNullable(dataMap.put(data.getPath(), localData));
    }

    @Override
    public Optional<ChildData> remove(String path)
    {
        return Optional.ofNullable(dataMap.remove(path));
    }

    @Override
    public Optional<ChildData> get(String path)
    {
        return Optional.ofNullable(dataMap.get(path));
    }

    @Override
    public int size()
    {
        return dataMap.size();
    }

    @Override
    public Stream<ChildData> stream()
    {
        return dataMap.values().stream();
    }

    @Override
    public Stream<ChildData> streamImmediateChildren(String fromParent)
    {
        return dataMap.entrySet()
            .stream()
            .filter(entry -> {
                ZKPaths.PathAndNode pathAndNode = ZKPaths.getPathAndNode(entry.getKey());
                return pathAndNode.getPath().equals(fromParent);
            })
            .map(Map.Entry::getValue);
    }

    @Override
    public void clear()
    {
        dataMap.clear();
    }
}