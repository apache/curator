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

import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.x.async.modeled.ModeledDetails;
import org.apache.curator.x.async.modeled.recipes.ModeledCachedNode;
import org.apache.curator.x.async.modeled.recipes.ModeledNodeCache;
import org.apache.zookeeper.data.Stat;
import java.util.Objects;
import java.util.Optional;

public class ModeledNodeCacheImpl<T> implements ModeledNodeCache<T>
{
    private final NodeCache cache;
    private final ModeledDetails<T> modeled;

    public ModeledNodeCacheImpl(ModeledDetails<T> modeled, NodeCache cache)
    {
        this.modeled = Objects.requireNonNull(modeled, "modeled cannot be null");
        this.cache = Objects.requireNonNull(cache, "cache cannot be null");
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
    }

    @Override
    public void start(boolean buildInitial)
    {
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
    public Listenable<NodeCacheListener> getListenable()
    {
        return cache.getListenable();
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
            return Optional.of(new ModeledCachedNodeImpl<T>(modeled.getPath(), null, stat));
        }
        return Optional.of(new ModeledCachedNodeImpl<>(modeled.getPath(), modeled.getSerializer().deserialize(data), stat));
    }

    @Override
    public void close()
    {
        CloseableUtils.closeQuietly(cache);
    }
}
