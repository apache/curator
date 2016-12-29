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
package org.apache.curator.framework.recipes.watch;

import com.google.common.cache.Cache;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

abstract class CuratorCacheBase implements CuratorCache
{
    protected final Cache<String, CachedNode> cache;

    protected CuratorCacheBase(Cache<String, CachedNode> cache)
    {
        this.cache = Objects.requireNonNull(cache, "cache cannot be null");
    }

    @Override
    public final boolean clear(String path)
    {
        return cache.asMap().remove(path) != null;
    }

    @Override
    public final void clearAll()
    {
        cache.invalidateAll();
    }

    @Override
    public final boolean exists(String path)
    {
        return cache.asMap().containsKey(path);
    }

    @Override
    public final Set<String> paths()
    {
        return cache.asMap().keySet();
    }

    @Override
    public final Collection<CachedNode> nodes()
    {
        return cache.asMap().values();
    }

    @Override
    public final Set<Map.Entry<String, CachedNode>> entries()
    {
        return cache.asMap().entrySet();
    }

    /**
     * As a memory optimization, you can clear the cached data bytes for a node. Subsequent
     * calls to {@link CachedNode#getData()} for this node will return <code>null</code>.
     *
     * @param path the path of the node to clear
     */
    @Override
    public final void clearDataBytes(String path)
    {
        clearDataBytes(path, -1);
    }

    /**
     * As a memory optimization, you can clear the cached data bytes for a node. Subsequent
     * calls to {@link CachedNode#getData()} for this node will return <code>null</code>.
     *
     * @param path  the path of the node to clear
     * @param ifVersion if non-negative, only clear the data if the data's version matches this version
     * @return true if the data was cleared
     */
    @Override
    public final boolean clearDataBytes(String path, int ifVersion)
    {
        CachedNode data = cache.asMap().get(path);
        if ( data != null )
        {
            if ( (ifVersion < 0) || ((data.getStat() != null) && (ifVersion == data.getStat().getVersion())) )
            {
                return cache.asMap().replace(path, data, new CachedNode(data.getStat()));
            }
        }
        return false;
    }
}
