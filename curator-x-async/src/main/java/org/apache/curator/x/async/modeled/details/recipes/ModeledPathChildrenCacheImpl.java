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
package org.apache.curator.x.async.modeled.details.recipes;

import com.google.common.util.concurrent.MoreExecutors;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.CloseableExecutorService;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.x.async.api.CreateOption;
import org.apache.curator.x.async.modeled.ModeledDetails;
import org.apache.curator.x.async.modeled.ZPath;
import org.apache.curator.x.async.modeled.recipes.ModeledCachedNode;
import org.apache.curator.x.async.modeled.recipes.ModeledPathChildrenCache;
import org.apache.curator.x.async.modeled.recipes.ModeledPathChildrenCacheEvent;
import org.apache.curator.x.async.modeled.recipes.ModeledPathChildrenCacheListener;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Collectors;

public class ModeledPathChildrenCacheImpl<T> implements ModeledPathChildrenCache<T>
{
    private final ModeledDetails<T> modeled;
    private final PathChildrenCache cache;
    private final Map<ModeledPathChildrenCacheListener, PathChildrenCacheListener> listenerMap = new ConcurrentHashMap<>();

    public ModeledPathChildrenCacheImpl(CuratorFramework client, ModeledDetails<T> modeled, boolean cacheData, ThreadFactory threadFactory, ExecutorService executorService, CloseableExecutorService closeableExecutorService)
    {
        this.modeled = modeled;
        PathChildrenCache localCache;
        if ( threadFactory != null )
        {
            localCache = new PathChildrenCache(client, modeled.getPath().fullPath(), cacheData, modeled.getCreateOptions().contains(CreateOption.compress), threadFactory);
        }
        else if ( executorService != null )
        {
            localCache = new PathChildrenCache(client, modeled.getPath().fullPath(), cacheData, modeled.getCreateOptions().contains(CreateOption.compress), executorService);
        }
        else if ( closeableExecutorService != null )
        {
            localCache = new PathChildrenCache(client, modeled.getPath().fullPath(), cacheData, modeled.getCreateOptions().contains(CreateOption.compress), closeableExecutorService);
        }
        else
        {
            localCache = new PathChildrenCache(client, modeled.getPath().fullPath(), cacheData, modeled.getCreateOptions().contains(CreateOption.compress), PathChildrenCache.defaultThreadFactory);
        }
        cache = localCache;
    }

    @Override
    public void start()
    {
        try
        {
            cache.start();
        }
        catch ( Exception e )
        {
            throw new RuntimeException("can't start cache", e);
        }
    }

    @Override
    public void start(PathChildrenCache.StartMode mode)
    {
        try
        {
            cache.start(mode);
        }
        catch ( Exception e )
        {
            throw new RuntimeException("can't start cache", e);
        }
    }

    @Override
    public void rebuild()
    {
        try
        {
            cache.rebuild();
        }
        catch ( Exception e )
        {
            throw new RuntimeException("can't rebuild cache", e);
        }
    }

    @Override
    public void rebuildNode(String fullPath)
    {
        try
        {
            cache.rebuildNode(fullPath);
        }
        catch ( Exception e )
        {
            throw new RuntimeException("can't rebuild cache at " + fullPath, e);
        }
    }

    @Override
    public Listenable<ModeledPathChildrenCacheListener> getListenable()
    {
        return new Listenable<ModeledPathChildrenCacheListener>()
        {
            @Override
            public void addListener(ModeledPathChildrenCacheListener listener)
            {
                addListener(listener, MoreExecutors.sameThreadExecutor());
            }

            @Override
            public void addListener(ModeledPathChildrenCacheListener listener, Executor executor)
            {
                PathChildrenCacheListener pathChildrenCacheListener = (client, event) -> {
                    ModeledPathChildrenCacheEvent modeledEvent = new ModeledPathChildrenCacheEvent()
                    {
                        @Override
                        public PathChildrenCacheEvent.Type getType()
                        {
                            return event.getType();
                        }

                        @Override
                        public ModeledCachedNode getNode()
                        {
                            return from(event.getData());
                        }
                    };
                    listener.event(modeledEvent);
                };
                listenerMap.put(listener, pathChildrenCacheListener);
                cache.getListenable().addListener(pathChildrenCacheListener);
            }

            @Override
            public void removeListener(ModeledPathChildrenCacheListener listener)
            {
                PathChildrenCacheListener pathChildrenCacheListener = listenerMap.remove(listener);
                if ( pathChildrenCacheListener != null )
                {
                    cache.getListenable().removeListener(pathChildrenCacheListener);
                }
            }
        };
    }

    @Override
    public List<ModeledCachedNode> getCurrentData()
    {
        return cache.getCurrentData().stream()
            .map(this::from)
            .collect(Collectors.toList());
    }

    private ModeledCachedNode<T> from(ChildData data)
    {
        if ( data == null )
        {
            return null;
        }
        T model = (data.getData() != null) ? modeled.getSerializer().deserialize(data.getData()) : null;
        return new ModeledCachedNode<>(ZPath.parse(data.getPath()), model, data.getStat());
    }

    @Override
    public ModeledCachedNode getCurrentData(String fullPath)
    {
        return from(cache.getCurrentData(fullPath));
    }

    @Override
    public void clearDataBytes(String fullPath)
    {
        cache.clearDataBytes(fullPath);
    }

    @Override
    public boolean clearDataBytes(String fullPath, int ifVersion)
    {
        return cache.clearDataBytes(fullPath, ifVersion);
    }

    @Override
    public void clearAndRefresh()
    {
        try
        {
            cache.clearAndRefresh();
        }
        catch ( Exception e )
        {
            throw new RuntimeException("could not clear and refresh", e);
        }
    }

    @Override
    public void clear()
    {
        cache.clear();
    }

    @Override
    public void close()
    {
        CloseableUtils.closeQuietly(cache);
    }
}
