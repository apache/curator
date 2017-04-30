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
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.x.async.AsyncStage;
import org.apache.curator.x.async.modeled.CachedModeledCuratorFramework;
import org.apache.curator.x.async.modeled.CuratorModelSpec;
import org.apache.curator.x.async.modeled.ModeledCuratorFramework;
import org.apache.curator.x.async.modeled.ZPath;
import org.apache.curator.x.async.modeled.recipes.ModeledCache;
import org.apache.curator.x.async.modeled.recipes.ModeledCachedNode;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.DataTree;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

class CachedModeledCuratorFrameworkImpl<T> implements CachedModeledCuratorFramework<T>
{
    private final ModeledCuratorFramework<T> client;
    private final ModeledCache<T> cache;
    private final ZPath path;

    CachedModeledCuratorFrameworkImpl(ModeledCuratorFramework<T> client, ModeledCache<T> cache, ZPath path)
    {
        this.client = Objects.requireNonNull(client, "client cannot be null");
        this.cache = Objects.requireNonNull(cache, "cache cannot be null");
        this.path = Objects.requireNonNull(path, "path cannot be null");
    }

    @Override
    public ModeledCache<T> getCache()
    {
        return cache;
    }

    @Override
    public void start()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public CuratorModelSpec<T> modelSpec()
    {
        return client.modelSpec();
    }

    @Override
    public CachedModeledCuratorFramework<T> cached(ModeledCache<T> cache)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public CachedModeledCuratorFramework<T> cached()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public CuratorFramework unwrap()
    {
        return client.unwrap();
    }

    @Override
    public ModeledCuratorFramework<T> at(String child)
    {
        return new CachedModeledCuratorFrameworkImpl<>(client.at(child), cache, path.at(child));
    }

    @Override
    public AsyncStage<String> set(T model)
    {
        return client.set(model);
    }

    @Override
    public AsyncStage<String> set(T model, Stat storingStatIn)
    {
        return client.set(model, storingStatIn);
    }

    @Override
    public AsyncStage<T> read()
    {
        return read(null);
    }

    @Override
    public AsyncStage<T> read(Stat storingStatIn)
    {
        Optional<ModeledCachedNode<T>> data = cache.getCurrentData(path);
        if ( data.isPresent() )
        {
            ModeledCachedNode<T> localData = data.get();
            T model = localData.getModel();
            if ( model != null )
            {
                if ( (storingStatIn != null) && (localData.getStat() != null) )
                {
                    DataTree.copyStat(localData.getStat(), storingStatIn);
                }
                return new ModelStage<>(model);
            }
        }
        return (storingStatIn != null) ? client.read(storingStatIn) : client.read();
    }

    @Override
    public AsyncStage<Stat> update(T model)
    {
        return client.update(model);
    }

    @Override
    public AsyncStage<Stat> update(T model, int version)
    {
        return client.update(model, version);
    }

    @Override
    public AsyncStage<Void> delete()
    {
        return client.delete();
    }

    @Override
    public AsyncStage<Void> delete(int version)
    {
        return client.delete(version);
    }

    @Override
    public AsyncStage<Stat> checkExists()
    {
        Optional<ModeledCachedNode<T>> data = cache.getCurrentData(path);
        return data.map(node -> (AsyncStage<Stat>)new ModelStage<>(node.getStat())).orElseGet(client::checkExists);
    }

    @Override
    public AsyncStage<List<ZPath>> getChildren()
    {
        return client.getChildren();
    }

    @Override
    public AsyncStage<Map<ZPath, AsyncStage<T>>> readChildren()
    {
        ModelStage<Map<ZPath, AsyncStage<T>>> modelStage = new ModelStage<>();
        client.getChildren().whenComplete((children, e) -> {
            if ( e != null )
            {
                modelStage.completeExceptionally(e);
            }
            else
            {
                Map<ZPath, AsyncStage<T>> map = children.stream().collect(Collectors.toMap(Function.identity(), path1 -> at(path1.nodeName()).read()));
                modelStage.complete(map);
            }
        });
        return modelStage;
    }

    @Override
    public CuratorOp createOp(T model)
    {
        return client.createOp(model);
    }

    @Override
    public CuratorOp updateOp(T model)
    {
        return client.updateOp(model);
    }

    @Override
    public CuratorOp updateOp(T model, int version)
    {
        return client.updateOp(model, version);
    }

    @Override
    public CuratorOp deleteOp()
    {
        return client.deleteOp();
    }

    @Override
    public CuratorOp deleteOp(int version)
    {
        return client.deleteOp(version);
    }

    @Override
    public CuratorOp checkExistsOp()
    {
        return client.checkExistsOp();
    }

    @Override
    public CuratorOp checkExistsOp(int version)
    {
        return client.checkExistsOp(version);
    }

    @Override
    public AsyncStage<List<CuratorTransactionResult>> inTransaction(List<CuratorOp> operations)
    {
        return client.inTransaction(operations);
    }
}
