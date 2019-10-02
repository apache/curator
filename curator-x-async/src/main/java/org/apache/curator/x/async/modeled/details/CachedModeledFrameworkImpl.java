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

import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.AsyncStage;
import org.apache.curator.x.async.modeled.ModelSpec;
import org.apache.curator.x.async.modeled.ModeledFramework;
import org.apache.curator.x.async.modeled.ZNode;
import org.apache.curator.x.async.modeled.ZPath;
import org.apache.curator.x.async.modeled.cached.CachedModeledFramework;
import org.apache.curator.x.async.modeled.cached.ModeledCache;
import org.apache.curator.x.async.modeled.cached.ModeledCacheListener;
import org.apache.curator.x.async.modeled.versioned.VersionedModeledFramework;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.DataTree;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

class CachedModeledFrameworkImpl<T> implements CachedModeledFramework<T>
{
    private final ModeledFramework<T> client;
    private final ModeledCacheImpl<T> cache;
    private final Executor executor;

    CachedModeledFrameworkImpl(ModeledFramework<T> client, ExecutorService executor)
    {
        this(client, new ModeledCacheImpl<>(client.unwrap().unwrap(), client.modelSpec(), executor), executor);
    }

    private CachedModeledFrameworkImpl(ModeledFramework<T> client, ModeledCacheImpl<T> cache, Executor executor)
    {
        this.client = client;
        this.cache = cache;
        this.executor = executor;
    }

    @Override
    public ModeledCache<T> cache()
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
    public Listenable<ModeledCacheListener<T>> listenable()
    {
        return cache.listenable();
    }

    @Override
    public CachedModeledFramework<T> cached()
    {
        throw new UnsupportedOperationException("Already a cached instance");
    }

    @Override
    public CachedModeledFramework<T> cached(ExecutorService executor)
    {
        throw new UnsupportedOperationException("Already a cached instance");
    }

    @Override
    public VersionedModeledFramework<T> versioned()
    {
        return new VersionedModeledFrameworkImpl<>(this);
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
    public CachedModeledFramework<T> child(Object child)
    {
        return new CachedModeledFrameworkImpl<>(client.child(child), cache, executor);
    }

    @Override
    public ModeledFramework<T> parent()
    {
        throw new UnsupportedOperationException("Not supported for CachedModeledFramework. Instead, call parent() on the ModeledFramework before calling cached()");
    }

    @Override
    public CachedModeledFramework<T> withPath(ZPath path)
    {
        return new CachedModeledFrameworkImpl<>(client.withPath(path), cache, executor);
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
    public AsyncStage<String> set(T model, Stat storingStatIn, int version)
    {
        return client.set(model, storingStatIn, version);
    }

    @Override
    public AsyncStage<String> set(T model, int version)
    {
        return client.set(model, version);
    }

    @Override
    public AsyncStage<T> read()
    {
        return internalRead(ZNode::model, this::exceptionally);
    }

    @Override
    public AsyncStage<T> read(Stat storingStatIn)
    {
        return internalRead(n -> {
            if ( storingStatIn != null )
            {
                DataTree.copyStat(n.stat(), storingStatIn);
            }
            return n.model();
        }, this::exceptionally);
    }

    @Override
    public AsyncStage<ZNode<T>> readAsZNode()
    {
        return internalRead(Function.identity(), this::exceptionally);
    }

    @Override
    public AsyncStage<T> readThrough()
    {
        return internalRead(ZNode::model, client::read);
    }

    @Override
    public AsyncStage<T> readThrough(Stat storingStatIn)
    {
        return internalRead(ZNode::model, () -> client.read(storingStatIn));
    }

    @Override
    public AsyncStage<ZNode<T>> readThroughAsZNode()
    {
        return internalRead(Function.identity(), client::readAsZNode);
    }

    @Override
    public AsyncStage<List<T>> list()
    {
        List<T> children = cache.currentChildren()
            .values()
            .stream()
            .map(ZNode::model)
            .collect(Collectors.toList());
        return ModelStage.completed(children);
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
        return data.map(node -> completed(node.stat())).orElseGet(() -> completed(null));
    }

    @Override
    public AsyncStage<List<ZPath>> children()
    {
        List<ZPath> paths = cache.currentChildren(client.modelSpec().path())
            .keySet()
            .stream()
            .filter(path -> !path.isRoot() && path.parent().equals(client.modelSpec().path()))
            .collect(Collectors.toList());
        return completed(paths);
    }

    @Override
    public AsyncStage<List<ZNode<T>>> childrenAsZNodes()
    {
        List<ZNode<T>> nodes = cache.currentChildren(client.modelSpec().path())
            .entrySet()
            .stream()
            .filter(e -> !e.getKey().isRoot() && e.getKey().parent().equals(client.modelSpec().path()))
            .map(Map.Entry::getValue)
            .collect(Collectors.toList());
        return completed(nodes);
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

    private <U> AsyncStage<U> completed(U value)
    {
        return ModelStage.completed(value);
    }

    private <U> AsyncStage<U> exceptionally()
    {
        KeeperException.NoNodeException exception = new KeeperException.NoNodeException(client.modelSpec().path().fullPath());
        return ModelStage.exceptionally(exception);
    }

    private <U> AsyncStage<U> internalRead(Function<ZNode<T>, U> resolver, Supplier<AsyncStage<U>> elseProc)
    {
        ZPath path = client.modelSpec().path();
        Optional<ZNode<T>> data = cache.currentData(path);
        return data.map(node -> completed(resolver.apply(node)))
            .orElseGet(elseProc);
    }
}
