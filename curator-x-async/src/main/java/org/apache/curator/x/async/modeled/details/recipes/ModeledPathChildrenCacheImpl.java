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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.x.async.modeled.ModelSerializer;
import org.apache.curator.x.async.modeled.ZPath;
import org.apache.curator.x.async.modeled.recipes.ModeledCacheEvent;
import org.apache.curator.x.async.modeled.recipes.ModeledCacheEventType;
import org.apache.curator.x.async.modeled.recipes.ModeledCacheListener;
import org.apache.curator.x.async.modeled.recipes.ModeledCachedNode;
import org.apache.curator.x.async.modeled.recipes.ModeledPathChildrenCache;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

public class ModeledPathChildrenCacheImpl<T> implements ModeledPathChildrenCache<T>
{
    private final PathChildrenCache cache;
    private final Map<ModeledCacheListener, PathChildrenCacheListener> listenerMap = new ConcurrentHashMap<>();
    private final ModelSerializer<T> serializer;

    public ModeledPathChildrenCacheImpl(PathChildrenCache cache, ModelSerializer<T> serializer)
    {
        this.cache = Objects.requireNonNull(cache, "cache cannot be null");
        this.serializer = Objects.requireNonNull(serializer, "serializer cannot be null");
    }

    @Override
    public PathChildrenCache unwrap()
    {
        return cache;
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
    public void rebuildNode(ZPath fullPath)
    {
        try
        {
            cache.rebuildNode(fullPath.fullPath());
        }
        catch ( Exception e )
        {
            throw new RuntimeException("can't rebuild cache at " + fullPath, e);
        }
    }

    @Override
    public Listenable<ModeledCacheListener<T>> getListenable()
    {
        return new Listenable<ModeledCacheListener<T>>()
        {
            @Override
            public void addListener(ModeledCacheListener<T> listener)
            {
                addListener(listener, MoreExecutors.sameThreadExecutor());
            }

            @Override
            public void addListener(ModeledCacheListener<T> listener, Executor executor)
            {
                PathChildrenCacheListener pathChildrenCacheListener = (client, event) -> {
                    ModeledCacheEventType eventType = toType(event.getType());
                    Optional<ModeledCachedNode<T>> node = Optional.ofNullable(from(serializer, event.getData()));
                    ModeledCacheEvent<T> modeledEvent = new ModeledCacheEvent<T>()
                    {
                        @Override
                        public ModeledCacheEventType getType()
                        {
                            return eventType;
                        }

                        @Override
                        public Optional<ModeledCachedNode<T>> getNode()
                        {
                            return node;
                        }
                    };
                    listener.event(modeledEvent);
                };
                listenerMap.put(listener, pathChildrenCacheListener);
                cache.getListenable().addListener(pathChildrenCacheListener);
            }

            @Override
            public void removeListener(ModeledCacheListener listener)
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
            .map(data -> from(serializer, data))
            .collect(Collectors.toList());
    }

    @Override
    public Optional<ModeledCachedNode> getCurrentData(String fullPath)
    {
        return Optional.ofNullable(from(serializer, cache.getCurrentData(fullPath)));
    }

    @Override
    public void clearDataBytes(ZPath fullPath)
    {
        cache.clearDataBytes(fullPath.fullPath());
    }

    @Override
    public boolean clearDataBytes(ZPath fullPath, int ifVersion)
    {
        return cache.clearDataBytes(fullPath.fullPath(), ifVersion);
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

    static <T> ModeledCachedNode<T> from(ModelSerializer<T> serializer, ChildData data)
    {
        if ( data == null )
        {
            return null;
        }
        T model = ((data.getData() != null) && (data.getData().length > 0)) ? serializer.deserialize(data.getData()) : null;
        return new ModeledCachedNodeImpl<>(ZPath.parse(data.getPath()), model, data.getStat());
    }

    @VisibleForTesting
    static ModeledCacheEventType toType(PathChildrenCacheEvent.Type type)
    {
        switch ( type )
        {
            case CHILD_ADDED:
                return ModeledCacheEventType.NODE_ADDED;

            case CHILD_UPDATED:
                return ModeledCacheEventType.NODE_UPDATED;

            case CHILD_REMOVED:
                return ModeledCacheEventType.NODE_REMOVED;

            case CONNECTION_SUSPENDED:
                return ModeledCacheEventType.CONNECTION_SUSPENDED;

            case CONNECTION_RECONNECTED:
                return ModeledCacheEventType.CONNECTION_RECONNECTED;

            case CONNECTION_LOST:
                return ModeledCacheEventType.CONNECTION_LOST;

            case INITIALIZED:
                return ModeledCacheEventType.INITIALIZED;
        }
        throw new UnsupportedOperationException("Unknown type: " + type);
    }
}
