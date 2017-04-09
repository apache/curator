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
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.x.async.modeled.ModeledDetails;
import org.apache.curator.x.async.modeled.ZPath;
import org.apache.curator.x.async.modeled.recipes.ModeledCacheEvent;
import org.apache.curator.x.async.modeled.recipes.ModeledCacheEventType;
import org.apache.curator.x.async.modeled.recipes.ModeledCacheListener;
import org.apache.curator.x.async.modeled.recipes.ModeledCachedNode;
import org.apache.curator.x.async.modeled.recipes.ModeledTreeCache;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import static org.apache.curator.x.async.modeled.details.recipes.ModeledPathChildrenCacheImpl.from;

public class ModeledTreeCacheImpl<T> implements ModeledTreeCache<T>
{
    private final ModeledDetails<T> modeled;
    private final TreeCache cache;
    private final Map<ModeledCacheListener, TreeCacheListener> listenerMap = new ConcurrentHashMap<>();

    public ModeledTreeCacheImpl(ModeledDetails<T> modeled, TreeCache cache)
    {
        this.modeled = Objects.requireNonNull(modeled, "modeled cannot be null");
        this.cache = Objects.requireNonNull(cache, "cache cannot be null");
    }

    @Override
    public TreeCache unwrap()
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
    }

    @Override
    public void close()
    {
        CloseableUtils.closeQuietly(cache);
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
                TreeCacheListener treeCacheListener = (client, event) -> {
                    ModeledCacheEvent<T> wrappedEvent = new ModeledCacheEvent<T>()
                    {
                        @Override
                        public ModeledCacheEventType getType()
                        {
                            return toType(event.getType());
                        }

                        @Override
                        public Optional<ModeledCachedNode<T>> getNode()
                        {
                            return Optional.ofNullable(from(modeled, event.getData()));
                        }
                    };
                };
                listenerMap.put(listener, treeCacheListener);
                cache.getListenable().addListener(treeCacheListener, executor);
            }

            @Override
            public void removeListener(ModeledCacheListener<T> listener)
            {
                TreeCacheListener treeCacheListener = listenerMap.remove(listener);
                if ( treeCacheListener != null )
                {
                    cache.getListenable().removeListener(treeCacheListener);
                }
            }
        };
    }

    @Override
    public Map<ZPath, ModeledCachedNode<T>> getCurrentChildren(ZPath fullPath)
    {
        Map<String, ChildData> currentChildren = cache.getCurrentChildren(fullPath.fullPath());
        if ( currentChildren == null )
        {
            return Collections.emptyMap();
        }
        return currentChildren.entrySet().stream()
            .map(entry -> new AbstractMap.SimpleEntry<>(ZPath.parse(entry.getKey()), from(modeled, entry.getValue())))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public Optional<ModeledCachedNode<T>> getCurrentData(ZPath fullPath)
    {
        return Optional.ofNullable(from(modeled, cache.getCurrentData(fullPath.fullPath())));
    }

    @VisibleForTesting
    static ModeledCacheEventType toType(TreeCacheEvent.Type type)
    {
        switch ( type )
        {
            case NODE_ADDED:
                return ModeledCacheEventType.NODE_ADDED;

            case NODE_UPDATED:
                return ModeledCacheEventType.NODE_UPDATED;

            case NODE_REMOVED:
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
