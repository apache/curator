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
package org.apache.curator.x.async.modeled.details;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.listen.ListenerContainer;
import org.apache.curator.framework.recipes.watch.CacheEvent;
import org.apache.curator.framework.recipes.watch.CacheListener;
import org.apache.curator.framework.recipes.watch.CacheSelectors;
import org.apache.curator.framework.recipes.watch.CachedNode;
import org.apache.curator.framework.recipes.watch.CuratorCache;
import org.apache.curator.framework.recipes.watch.CuratorCacheBuilder;
import org.apache.curator.utils.ThreadUtils;
import org.apache.curator.x.async.api.CreateOption;
import org.apache.curator.x.async.modeled.ModelSerializer;
import org.apache.curator.x.async.modeled.ModelSpec;
import org.apache.curator.x.async.modeled.ZNode;
import org.apache.curator.x.async.modeled.ZPath;
import org.apache.curator.x.async.modeled.cached.ModeledCache;
import org.apache.curator.x.async.modeled.cached.ModeledCacheListener;
import org.apache.zookeeper.data.Stat;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

class ModeledCacheImpl<T> implements CacheListener, ModeledCache<T>
{
    private final CuratorCache cache;
    private final Map<ZPath, Entry<T>> entries = new ConcurrentHashMap<>();
    private final ModelSerializer<T> serializer;
    private final ListenerContainer<ModeledCacheListener<T>> listenerContainer = new ListenerContainer<>();
    private final ZPath basePath;

    private static final class Entry<T>
    {
        final Stat stat;
        final T model;

        Entry(Stat stat, T model)
        {
            this.stat = stat;
            this.model = model;
        }
    }

    ModeledCacheImpl(CuratorFramework client, ModelSpec<T> modelSpec, ExecutorService executor)
    {
        if ( !modelSpec.path().isResolved() && !modelSpec.path().isRoot() && modelSpec.path().parent().isResolved() )
        {
            modelSpec = modelSpec.parent(); // i.e. the last item is a parameter
        }

        basePath = modelSpec.path();
        this.serializer = modelSpec.serializer();
        boolean dataIsCompressed = modelSpec.createOptions().contains(CreateOption.compress);
        cache = CuratorCacheBuilder.builder(client, basePath.fullPath())
            .withCacheSelector(dataIsCompressed ? CacheSelectors.uncompressedStatAndData() : CacheSelectors.statAndData())
            .sendingRefreshEvents(true)
            .build();
    }

    public void start()
    {
        try
        {
            cache.getListenable().addListener(this);
            cache.start();
        }
        catch ( Exception e )
        {
            throw new RuntimeException(e);
        }
    }

    public void close()
    {
        cache.getListenable().removeListener(this);
        cache.close();
        entries.clear();
    }

    @Override
    public Optional<ZNode<T>> currentData(ZPath path)
    {
        Entry<T> entry = entries.remove(path);
        if ( entry != null )
        {
            return Optional.of(new ZNodeImpl<>(path, entry.stat, entry.model));
        }
        return Optional.empty();
    }

    ZPath basePath()
    {
        return basePath;
    }

    Map<ZPath, ZNode<T>> currentChildren()
    {
        return currentChildren(basePath);
    }

    @Override
    public Map<ZPath, ZNode<T>> currentChildren(ZPath path)
    {
        return entries.entrySet()
            .stream()
            .filter(entry -> entry.getKey().startsWith(path))
            .map(entry -> new AbstractMap.SimpleEntry<>(entry.getKey(), new ZNodeImpl<>(entry.getKey(), entry.getValue().stat, entry.getValue().model)))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public Listenable<ModeledCacheListener<T>> listenable()
    {
        return listenerContainer;
    }

    @Override
    public void process(CacheEvent event, String path, CachedNode affectedNode)
    {
        try
        {
            internalChildEvent(event, path, affectedNode);
        }
        catch ( Exception e )
        {
            ThreadUtils.checkInterrupted(e);

            listenerContainer.forEach(l -> {
                l.handleException(e);
                return null;
            });
        }
    }

    private void internalChildEvent(CacheEvent event, String pathStr, CachedNode affectedNode) throws Exception
    {
        switch ( event )
        {
        case NODE_CREATED:
        case NODE_CHANGED:
        {
            ZPath path = ZPath.parse(pathStr);
            if ( !path.equals(basePath) )
            {
                byte[] bytes = affectedNode.getData();
                if ( (bytes != null) && (bytes.length > 0) )    // otherwise it's probably just a parent node being created
                {
                    T model = serializer.deserialize(bytes);
                    entries.put(path, new Entry<>(affectedNode.getStat(), model));
                    ModeledCacheListener.Type type = (event == CacheEvent.NODE_CREATED) ? ModeledCacheListener.Type.NODE_ADDED : ModeledCacheListener.Type.NODE_UPDATED;
                    accept(type, path, affectedNode.getStat(), model);
                }
            }
            break;
        }

        case NODE_DELETED:
        {
            ZPath path = ZPath.parse(pathStr);
            if ( !path.equals(basePath) )
            {
                Entry<T> entry = entries.remove(path);
                T model = (entry != null) ? entry.model : serializer.deserialize(affectedNode.getData());
                Stat stat = (entry != null) ? entry.stat : affectedNode.getStat();
                accept(ModeledCacheListener.Type.NODE_REMOVED, path, stat, model);
            }
            break;
        }

        case CACHE_REFRESHED:
        {
            listenerContainer.forEach(l -> {
                l.initialized();
                return null;
            });
            break;
        }

        default:
            // ignore
            break;
        }
    }

    private void accept(ModeledCacheListener.Type type, ZPath path, Stat stat, T model)
    {
        listenerContainer.forEach(l -> {
            l.accept(type, path, stat, model);
            return null;
        });
    }
}
