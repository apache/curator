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
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.x.async.modeled.ModelSerializer;
import org.apache.curator.x.async.modeled.ZPath;
import org.apache.curator.x.async.modeled.recipes.ModeledCacheEvent;
import org.apache.curator.x.async.modeled.recipes.ModeledCacheEventType;
import org.apache.curator.x.async.modeled.recipes.ModeledCacheListener;
import org.apache.curator.x.async.modeled.recipes.ModeledCachedNode;
import org.apache.curator.x.async.modeled.recipes.ModeledNodeCache;
import org.apache.zookeeper.data.Stat;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

public class ModeledNodeCacheImpl<T> implements ModeledNodeCache<T>, ConnectionStateListener
{
    private final NodeCache cache;
    private final ModelSerializer<T> serializer;
    private final ZPath path;
    private final Map<ModeledCacheListener<T>, NodeCacheListener> listenerMap = new ConcurrentHashMap<>();

    public ModeledNodeCacheImpl(NodeCache cache, ModelSerializer<T> serializer)
    {
        this.cache = Objects.requireNonNull(cache, "cache cannot be null");
        this.serializer = Objects.requireNonNull(serializer, "serializer cannot be null");
        path = ZPath.parse(cache.getPath());
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState)
    {
        ModeledCacheEventType mappedType;
        switch ( newState )
        {
            default:
            {
                mappedType = null;
                break;
            }

            case RECONNECTED:
            case CONNECTED:
            {
                mappedType = ModeledCacheEventType.CONNECTION_RECONNECTED;
                break;
            }

            case SUSPENDED:
            {
                mappedType = ModeledCacheEventType.CONNECTION_SUSPENDED;
                break;
            }

            case LOST:
            {
                mappedType = ModeledCacheEventType.CONNECTION_LOST;
                break;
            }
        }

        if ( mappedType != null )
        {
            ModeledCacheEvent<T> event = new ModeledCacheEvent<T>()
            {
                @Override
                public ModeledCacheEventType getType()
                {
                    return mappedType;
                }

                @Override
                public Optional<ModeledCachedNode<T>> getNode()
                {
                    return Optional.empty();
                }
            };
            listenerMap.keySet().forEach(l -> l.event(null));
        }
    }

    @Override
    public NodeCache unwrap()
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
            throw new RuntimeException("Could not start", e);
        }
        cache.getClient().getConnectionStateListenable().addListener(this);
    }

    @Override
    public void start(boolean buildInitial)
    {
        cache.getClient().getConnectionStateListenable().removeListener(this);
        try
        {
            cache.start(buildInitial);
        }
        catch ( Exception e )
        {
            throw new RuntimeException("Could not start", e);
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
            throw new RuntimeException("Could not rebuild", e);
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
                NodeCacheListener nodeCacheListener = () ->
                {
                    Optional<ModeledCachedNode<T>> currentData = getCurrentData();
                    ModeledCacheEvent<T> event = new ModeledCacheEvent<T>()
                    {
                        @Override
                        public ModeledCacheEventType getType()
                        {
                            return currentData.isPresent() ? ModeledCacheEventType.NODE_UPDATED : ModeledCacheEventType.NODE_REMOVED;
                        }

                        @Override
                        public Optional<ModeledCachedNode<T>> getNode()
                        {
                            return currentData;
                        }
                    };
                    listener.event(event);
                };
                listenerMap.put(listener, nodeCacheListener);
                cache.getListenable().addListener(nodeCacheListener, executor);
            }

            @Override
            public void removeListener(ModeledCacheListener<T> listener)
            {
                NodeCacheListener nodeCacheListener = listenerMap.remove(listener);
                if ( nodeCacheListener != null )
                {
                    cache.getListenable().removeListener(nodeCacheListener);
                }
            }
        };
    }

    @Override
    public Optional<ModeledCachedNode<T>> getCurrentData()
    {
        ChildData currentData = cache.getCurrentData();
        if ( currentData == null )
        {
            return Optional.empty();
        }
        byte[] data = currentData.getData();
        Stat stat = currentData.getStat();
        if ( stat == null )
        {
            stat = new Stat();
        }
        if ( (data == null) || (data.length == 0) )
        {
            return Optional.of(new ModeledCachedNodeImpl<T>(path, null, stat));
        }
        return Optional.of(new ModeledCachedNodeImpl<>(path, serializer.deserialize(data), stat));
    }

    @Override
    public void close()
    {
        CloseableUtils.closeQuietly(cache);
    }
}
