/*
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

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
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

class CachedModeledFrameworkImpl<T> implements CachedModeledFramework<T> {
    private final ModeledFramework<T> client;
    private final ModeledCacheImpl<T> cache;
    private final Executor executor;
    private final ModelStage<Void> init;

    CachedModeledFrameworkImpl(ModeledFramework<T> client, ExecutorService executor) {
        this(
                client,
                new ModeledCacheImpl<>(client.unwrap().unwrap(), client.modelSpec(), executor),
                executor,
                ModelStage.make());
        listenable().addListener(new ModeledCacheListener<T>() {
            @Override
            public void accept(Type type, ZPath path, Stat stat, Object model) {
                // NOP
            }

            @Override
            public void initialized() {
                init.complete(null);
                ModeledCacheListener.super.initialized();
            }

            @Override
            public void handleException(Exception e) {
                init.completeExceptionally(e);
                ModeledCacheListener.super.handleException(e);
            }
        });
    }

    private CachedModeledFrameworkImpl(
            ModeledFramework<T> client, ModeledCacheImpl<T> cache, Executor executor, ModelStage<Void> init) {
        this.client = client;
        this.cache = cache;
        this.executor = executor;
        this.init = init;
    }

    @Override
    public ModeledCache<T> cache() {
        return cache;
    }

    @Override
    public void start() {
        cache.start();
    }

    @Override
    public void close() {
        cache.close();
    }

    @Override
    public Listenable<ModeledCacheListener<T>> listenable() {
        return cache.listenable();
    }

    @Override
    public CachedModeledFramework<T> cached() {
        throw new UnsupportedOperationException("Already a cached instance");
    }

    @Override
    public CachedModeledFramework<T> cached(ExecutorService executor) {
        throw new UnsupportedOperationException("Already a cached instance");
    }

    @Override
    public VersionedModeledFramework<T> versioned() {
        return new VersionedModeledFrameworkImpl<>(this);
    }

    @Override
    public AsyncCuratorFramework unwrap() {
        return client.unwrap();
    }

    @Override
    public ModelSpec<T> modelSpec() {
        return client.modelSpec();
    }

    @Override
    public CachedModeledFramework<T> child(Object child) {
        return new CachedModeledFrameworkImpl<>(client.child(child), cache, executor, init);
    }

    @Override
    public ModeledFramework<T> parent() {
        throw new UnsupportedOperationException(
                "Not supported for CachedModeledFramework. Instead, call parent() on the ModeledFramework before calling cached()");
    }

    @Override
    public CachedModeledFramework<T> withPath(ZPath path) {
        return new CachedModeledFrameworkImpl<>(client.withPath(path), cache, executor, init);
    }

    @Override
    public AsyncStage<String> set(T model) {
        return client.set(model);
    }

    @Override
    public AsyncStage<String> set(T model, Stat storingStatIn) {
        return client.set(model, storingStatIn);
    }

    @Override
    public AsyncStage<String> set(T model, Stat storingStatIn, int version) {
        return client.set(model, storingStatIn, version);
    }

    @Override
    public AsyncStage<String> set(T model, int version) {
        return client.set(model, version);
    }

    @Override
    public AsyncStage<T> read() {
        return internalRead(ZNode::model);
    }

    @Override
    public AsyncStage<T> read(Stat storingStatIn) {
        return internalRead(n -> {
            if (storingStatIn != null) {
                DataTree.copyStat(n.stat(), storingStatIn);
            }
            return n.model();
        });
    }

    @Override
    public AsyncStage<ZNode<T>> readAsZNode() {
        return internalRead(Function.identity());
    }

    @Override
    public AsyncStage<T> readThrough() {
        return internalRead(ZNode::model, client::read);
    }

    @Override
    public AsyncStage<T> readThrough(Stat storingStatIn) {
        return internalRead(ZNode::model, () -> client.read(storingStatIn));
    }

    @Override
    public AsyncStage<ZNode<T>> readThroughAsZNode() {
        return internalRead(Function.identity(), client::readAsZNode);
    }

    @Override
    public AsyncStage<List<T>> list() {
        return this.internalList(entry -> entry.getValue().model());
    }

    @Override
    public AsyncStage<Stat> update(T model) {
        return client.update(model);
    }

    @Override
    public AsyncStage<Stat> update(T model, int version) {
        return client.update(model, version);
    }

    @Override
    public AsyncStage<Void> delete() {
        return client.delete();
    }

    @Override
    public AsyncStage<Void> delete(int version) {
        return client.delete(version);
    }

    @Override
    public AsyncStage<Stat> checkExists() {
        return internalRead(ZNode::stat, () -> ModelStage.completed(null));
    }

    @Override
    public AsyncStage<List<ZPath>> children() {
        return this.internalChildren(Map.Entry::getKey);
    }

    @Override
    public AsyncStage<List<ZNode<T>>> childrenAsZNodes() {
        return this.internalChildren(Map.Entry::getValue);
    }

    @Override
    public CuratorOp createOp(T model) {
        return client.createOp(model);
    }

    @Override
    public CuratorOp updateOp(T model) {
        return client.updateOp(model);
    }

    @Override
    public CuratorOp updateOp(T model, int version) {
        return client.updateOp(model, version);
    }

    @Override
    public CuratorOp deleteOp() {
        return client.deleteOp();
    }

    @Override
    public CuratorOp deleteOp(int version) {
        return client.deleteOp(version);
    }

    @Override
    public CuratorOp checkExistsOp() {
        return client.checkExistsOp();
    }

    @Override
    public CuratorOp checkExistsOp(int version) {
        return client.checkExistsOp(version);
    }

    @Override
    public AsyncStage<List<CuratorTransactionResult>> inTransaction(List<CuratorOp> operations) {
        return client.inTransaction(operations);
    }

    private <U> AsyncStage<U> internalRead(Function<ZNode<T>, U> resolver) {
        return internalRead(resolver, null);
    }

    private <U> AsyncStage<U> internalRead(Function<ZNode<T>, U> resolver, Supplier<AsyncStage<U>> defaultSupplier) {
        ModelStage<U> stage = ModelStage.make();
        init.whenComplete((__, throwable) -> {
            if (throwable == null) {
                ZPath path = client.modelSpec().path();
                ZNode<T> zNode = cache.currentData(path).orElse(null);
                if (zNode == null) {
                    if (defaultSupplier == null) {
                        stage.completeExceptionally(new KeeperException.NoNodeException(
                                client.modelSpec().path().fullPath()));
                    } else {
                        defaultSupplier.get().whenComplete((elseData, elseThrowable) -> {
                            if (elseThrowable == null) {
                                stage.complete(elseData);
                            } else {
                                stage.completeExceptionally(elseThrowable);
                            }
                        });
                    }
                } else {
                    stage.complete(resolver.apply(zNode));
                }
            } else {
                stage.completeExceptionally(throwable);
            }
        });
        return stage;
    }

    private <U> ModelStage<List<U>> internalList(Function<Map.Entry<ZPath, ZNode<T>>, U> resolver) {
        return internalChildren(resolver, __ -> true);
    }

    private <U> ModelStage<List<U>> internalChildren(Function<Map.Entry<ZPath, ZNode<T>>, U> resolver) {
        return internalChildren(resolver, e -> e.getKey().parent().equals(client.modelSpec().path()));
    }

    private <U> ModelStage<List<U>> internalChildren(Function<Map.Entry<ZPath, ZNode<T>>, U> resolver,
                                                     Predicate<Map.Entry<ZPath, ZNode<T>>> filter) {
        ModelStage<List<U>> stage = ModelStage.make();
        init.whenComplete((__, throwable) -> {
            if (throwable == null) {
                stage.complete(cache.currentChildren().entrySet().stream()
                        .filter(filter)
                        .map(resolver)
                        .collect(Collectors.toList()));
            } else {
                stage.completeExceptionally(throwable);
            }
        });
        return stage;
    }
}
