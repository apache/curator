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

import com.google.common.collect.Lists;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.AsyncStage;
import org.apache.curator.x.async.modeled.ModelSpec;
import org.apache.curator.x.async.modeled.ModeledFramework;
import org.apache.curator.x.async.modeled.ZPath;
import org.apache.curator.x.async.modeled.cached.CachedModeledFramework;
import org.apache.curator.x.async.modeled.cached.ModeledCache;
import org.apache.curator.x.async.modeled.cached.ZNode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.DataTree;
import java.util.List;
import java.util.Optional;
import java.util.Set;

class CachedModeledFrameworkImpl<T> implements CachedModeledFramework<T>
{
    private final ModeledFramework<T> client;
    private final ModeledCacheImpl<T> cache;

    CachedModeledFrameworkImpl(ModeledFramework<T> client)
    {
        this(client, new ModeledCacheImpl<>(client.unwrap().unwrap(), client.modelSpec()));
    }

    private CachedModeledFrameworkImpl(ModeledFramework<T> client, ModeledCacheImpl<T> cache)
    {
        this.client = client;
        this.cache = cache;
    }

    @Override
    public ModeledCache<T> getCache()
    {
        return cache;
    }

    @Override
    public void start()
    {
        cache.start();
    }

    @Override
    public void close()
    {
        cache.close();
    }

    @Override
    public CachedModeledFramework<T> cached()
    {
        return this;
    }

    @Override
    public AsyncCuratorFramework unwrap()
    {
        return client.unwrap();
    }

    @Override
    public ModelSpec<T> modelSpec()
    {
        return client.modelSpec();
    }

    @Override
    public CachedModeledFramework<T> at(Object child)
    {
        return new CachedModeledFrameworkImpl<>(client.at(child), cache);
    }

    @Override
    public CachedModeledFramework<T> withPath(ZPath path)
    {
        return new CachedModeledFrameworkImpl<>(client.withPath(path), cache);
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
        ZPath path = client.modelSpec().path();
        Optional<ZNode<T>> data = cache.currentData(path);
        return data.map(node -> {
            if ( storingStatIn != null )
            {
                DataTree.copyStat(node.stat(), storingStatIn);
            }
            return new ModelStage<>(node.model());
        }).orElseGet(() -> new ModelStage<>(new KeeperException.NoNodeException(path.fullPath())));
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
        ZPath path = client.modelSpec().path();
        Optional<ZNode<T>> data = cache.currentData(path);
        return data.map(node -> new ModelStage<>(node.stat())).orElseGet(() -> new ModelStage<>((Stat)null));
    }

    @Override
    public AsyncStage<List<ZPath>> children()
    {
        Set<ZPath> paths = cache.currentChildren(client.modelSpec().path()).keySet();
        return new ModelStage<>(Lists.newArrayList(paths));
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
