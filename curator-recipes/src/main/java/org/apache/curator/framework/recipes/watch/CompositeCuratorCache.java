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

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.listen.ListenerContainer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

public class CompositeCuratorCache implements CuratorCache
{
    private final Map<String, CuratorCache> caches;
    private final ListenerContainer<CacheListener> listeners = new ListenerContainer<>();
    private final CacheListener listener = new CacheListener()
    {
        @Override
        public void process(final CacheEvent event, final String path, final CachedNode affectedNode)
        {
            Function<CacheListener, Void> proc = new Function<CacheListener, Void>()
            {
                @Override
                public Void apply(CacheListener listener)
                {
                    listener.process(event, path, affectedNode);
                    return null;
                }
            };
            listeners.forEach(proc);
        }
    };

    public CompositeCuratorCache(CuratorCache... caches)
    {
        this(toMap(Arrays.asList(caches)));
    }

    public CompositeCuratorCache(Collection<CuratorCache> caches)
    {
        this(toMap(caches));
    }

    private static Map<String, CuratorCache> toMap(Collection<CuratorCache> caches)
    {
        Map<String, CuratorCache> map = new HashMap<>();
        for ( CuratorCache cache : caches )
        {
            map.put(Integer.toString(map.size()), cache);
        }
        return map;
    }

    public CompositeCuratorCache(Map<String, CuratorCache> caches)
    {
        this.caches = ImmutableMap.copyOf(caches);
    }

    public Iterable<String> cacheKeys()
    {
        return caches.keySet();
    }

    public CuratorCache getCache(String key)
    {
        return caches.get(key);
    }

    @Override
    public CountDownLatch start()
    {
        List<CountDownLatch> latches = new ArrayList<>();
        for ( CuratorCache cache : caches.values() )
        {
            latches.add(cache.start());
        }
        return new CompositeCountDownLatch(latches);
    }

    @Override
    public void close()
    {
        for ( CuratorCache cache : caches.values() )
        {
            cache.getListenable().removeListener(listener);
            cache.close();
        }
    }

    @Override
    public Listenable<CacheListener> getListenable()
    {
        return listeners;
    }

    @Override
    public CountDownLatch refreshAll()
    {
        List<CountDownLatch> latches = new ArrayList<>();
        for ( CuratorCache cache : caches.values() )
        {
            latches.add(cache.refreshAll());
        }
        return new CompositeCountDownLatch(latches);
    }

    @Override
    public CountDownLatch refresh(String path)
    {
        List<CountDownLatch> latches = new ArrayList<>();
        for ( CuratorCache cache : caches.values() )
        {
            latches.add(cache.refresh(path));
        }
        return new CompositeCountDownLatch(latches);
    }

    @Override
    public boolean clear(String path)
    {
        boolean cleared = false;
        for ( CuratorCache cache : caches.values() )
        {
            if ( cache.clear(path) )
            {
                cleared = true;
            }
        }
        return cleared;
    }

    @Override
    public void clearAll()
    {
        for ( CuratorCache cache : caches.values() )
        {
            cache.clearAll();
        }
    }

    @Override
    public boolean exists(String path)
    {
        for ( CuratorCache cache : caches.values() )
        {
            if ( cache.exists(path) )
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public Set<String> paths()
    {
        Set<String> paths = new HashSet<>();
        for ( CuratorCache cache : caches.values() )
        {
            cache.clearAll();
        }
        return paths;
    }

    @Override
    public CachedNode get(String path)
    {
        for ( CuratorCache cache : caches.values() )
        {
            CachedNode node = cache.get(path);
            if ( node != null )
            {
                return node;
            }
        }
        return null;
    }

    @Override
    public Iterable<CachedNode> getAll()
    {
        List<Iterable<CachedNode>> nodes = new ArrayList<>();
        for ( CuratorCache cache : caches.values() )
        {
            nodes.add(cache.getAll());
        }
        return Iterables.concat(nodes);
    }

    @Override
    public Iterable<Map.Entry<String, CachedNode>> entries()
    {
        List<Iterable<Map.Entry<String, CachedNode>>> nodes = new ArrayList<>();
        for ( CuratorCache cache : caches.values() )
        {
            nodes.add(cache.entries());
        }
        return Iterables.concat(nodes);
    }

    @Override
    public boolean isEmpty()
    {
        for ( CuratorCache cache : caches.values() )
        {
            if ( !cache.isEmpty() )
            {
                return false;
            }
        }
        return true;
    }

    @Override
    public int size()
    {
        long size = 0;
        for ( CuratorCache cache : caches.values() )
        {
            size += cache.size();
        }
        return (int)Math.min(size, Integer.MAX_VALUE);
    }

    @Override
    public void clearDataBytes(String path)
    {
        for ( CuratorCache cache : caches.values() )
        {
            cache.clearDataBytes(path);
        }
    }

    @Override
    public boolean clearDataBytes(String path, int ifVersion)
    {
        boolean cleared = false;
        for ( CuratorCache cache : caches.values() )
        {
            if ( cache.clearDataBytes(path, ifVersion) )
            {
                cleared = true;
            }
        }
        return cleared;
    }

    @Override
    public long refreshCount()
    {
        long count = 0;
        for ( CuratorCache cache : caches.values() )
        {
            count = Math.min(count, cache.refreshCount());
        }
        return count;
    }
}
