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
import com.google.common.cache.CacheBuilder;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A CacheNodeMap that uses Guava's {@link com.google.common.cache.Cache} internally
 */
public class GuavaCachedNodeMap implements CachedNodeMap
{
    private final Cache<String, CachedNode> cache;

    /**
     * The factory for {@link org.apache.curator.framework.recipes.watch.GuavaCachedNodeMap}
     */
    public static final CachedNodeMapFactory factory = new CachedNodeMapFactory()
    {
        @Override
        public CachedNodeMap create(boolean weakValues, boolean softValues, Long expiresAfterWriteMs, Long expiresAfterAccessMs)
        {
            return new GuavaCachedNodeMap(weakValues, softValues, expiresAfterWriteMs, expiresAfterAccessMs);
        }
    };

    public GuavaCachedNodeMap(boolean weakValues, boolean softValues, Long expiresAfterWriteMs, Long expiresAfterAccessMs)
    {
        CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
        if ( weakValues )
        {
            cacheBuilder = cacheBuilder.weakValues();
        }
        if ( softValues )
        {
            cacheBuilder = cacheBuilder.softValues();
        }
        if ( expiresAfterWriteMs != null )
        {
            cacheBuilder = cacheBuilder.expireAfterWrite(expiresAfterWriteMs, TimeUnit.MILLISECONDS);
        }
        if ( expiresAfterAccessMs != null )
        {
            cacheBuilder = cacheBuilder.expireAfterAccess(expiresAfterAccessMs, TimeUnit.MILLISECONDS);
        }
        cache = cacheBuilder.build();
    }

    @Override
    public boolean isEmpty()
    {
        return cache.asMap().isEmpty();
    }

    @Override
    public int size()
    {
        return cache.asMap().size();
    }

    @Override
    public CachedNode remove(String path)
    {
        return cache.asMap().remove(path);
    }

    @Override
    public void invalidateAll()
    {
        cache.invalidateAll();
    }

    @Override
    public boolean containsKey(String path)
    {
        return cache.asMap().containsKey(path);
    }

    @Override
    public Collection<? extends String> pathSet()
    {
        return Collections.unmodifiableSet(cache.asMap().keySet());
    }

    @Override
    public Map<String, CachedNode> view()
    {
        return Collections.unmodifiableMap(cache.asMap());
    }

    @Override
    public Iterable<? extends Map.Entry<String, CachedNode>> entrySet()
    {
        return view().entrySet();
    }

    @Override
    public CachedNode get(String path)
    {
        return cache.asMap().get(path);
    }

    @Override
    public boolean replace(String path, CachedNode oldNode, CachedNode newNode)
    {
        return cache.asMap().replace(path, oldNode, newNode);
    }

    @Override
    public CachedNode put(String path, CachedNode node)
    {
        return cache.asMap().put(path, node);
    }

    @Override
    public void cleanUp()
    {
        cache.cleanUp();
    }
}
