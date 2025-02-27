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
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
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
import org.apache.zookeeper.data.Stat;

public class InitializedCachedModeledFramework<T> implements CachedModeledFramework<T> {

    private final CachedModeledFramework<T> framework;
    private final ModelStage<Void> init;

    private InitializedCachedModeledFramework(CachedModeledFramework<T> framework, ModelStage<Void> init) {
        this.framework = framework;
        this.init = init;
    }

    InitializedCachedModeledFramework(CachedModeledFramework<T> framework) {
        this.framework = framework;
        init = ModelStage.make();
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

    @Override
    public ModeledCache<T> cache() {
        return framework.cache();
    }

    @Override
    public void start() {
        framework.start();
    }

    @Override
    public void close() {
        framework.close();
    }

    @Override
    public Listenable<ModeledCacheListener<T>> listenable() {
        return framework.listenable();
    }

    @Override
    public AsyncStage<List<ZNode<T>>> childrenAsZNodes() {
        return internalRead(framework::childrenAsZNodes);
    }

    @Override
    public CuratorOp createOp(T model) {
        return framework.createOp(model);
    }

    @Override
    public CuratorOp updateOp(T model) {
        return framework.updateOp(model);
    }

    @Override
    public CuratorOp updateOp(T model, int version) {
        return framework.updateOp(model, version);
    }

    @Override
    public CuratorOp deleteOp() {
        return framework.deleteOp();
    }

    @Override
    public CuratorOp deleteOp(int version) {
        return framework.deleteOp(version);
    }

    @Override
    public CuratorOp checkExistsOp() {
        return framework.checkExistsOp();
    }

    @Override
    public CuratorOp checkExistsOp(int version) {
        return framework.checkExistsOp(version);
    }

    @Override
    public AsyncStage<List<CuratorTransactionResult>> inTransaction(List<CuratorOp> operations) {
        return framework.inTransaction(operations);
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
        return framework.unwrap();
    }

    @Override
    public ModelSpec<T> modelSpec() {
        return framework.modelSpec();
    }

    @Override
    public CachedModeledFramework<T> child(Object child) {
        return new InitializedCachedModeledFramework<>(framework.child(child), init);
    }

    @Override
    public ModeledFramework<T> parent() {
        throw new UnsupportedOperationException(
                "Not supported for CachedModeledFramework. Instead, call parent() on the ModeledFramework before calling cached()");
    }

    @Override
    public CachedModeledFramework<T> withPath(ZPath path) {
        return new InitializedCachedModeledFramework<>(framework.withPath(path), init);
    }

    @Override
    public AsyncStage<String> set(T model) {
        return framework.set(model);
    }

    @Override
    public AsyncStage<String> set(T model, int version) {
        return framework.set(model, version);
    }

    @Override
    public AsyncStage<String> set(T model, Stat storingStatIn) {
        return framework.set(model, storingStatIn);
    }

    @Override
    public AsyncStage<String> set(T model, Stat storingStatIn, int version) {
        return framework.set(model, storingStatIn, version);
    }

    @Override
    public AsyncStage<T> read() {
        return internalRead(framework::read);
    }

    @Override
    public AsyncStage<T> read(Stat storingStatIn) {
        return internalRead(() -> framework.read(storingStatIn));
    }

    @Override
    public AsyncStage<ZNode<T>> readAsZNode() {
        return internalRead(framework::readAsZNode);
    }

    @Override
    public AsyncStage<Stat> update(T model) {
        return framework.update(model);
    }

    @Override
    public AsyncStage<Stat> update(T model, int version) {
        return framework.update(model, version);
    }

    @Override
    public AsyncStage<Void> delete() {
        return framework.delete();
    }

    @Override
    public AsyncStage<Void> delete(int version) {
        return framework.delete(version);
    }

    @Override
    public AsyncStage<Stat> checkExists() {
        return framework.checkExists();
    }

    @Override
    public AsyncStage<List<ZPath>> children() {
        return internalRead(framework::children);
    }

    @Override
    public AsyncStage<T> readThrough() {
        return internalRead(framework::readThrough);
    }

    @Override
    public AsyncStage<T> readThrough(Stat storingStatIn) {
        return internalRead(() -> framework.readThrough(storingStatIn));
    }

    @Override
    public AsyncStage<ZNode<T>> readThroughAsZNode() {
        return internalRead(framework::readThroughAsZNode);
    }

    @Override
    public AsyncStage<List<T>> list() {
        return internalRead(framework::list);
    }

    @Override
    public CachedModeledFramework<T> initialized() {
        throw new UnsupportedOperationException("Already an initialized instance");
    }

    private <U> AsyncStage<U> internalRead(Supplier<AsyncStage<U>> innerSupplier) {
        ModelStage<U> stage = ModelStage.make();
        init.whenComplete((data, throwable) -> {
            if (throwable == null) {
                innerSupplier.get().whenComplete((data1, throwable1) -> {
                    if (throwable1 == null) {
                        stage.complete(data1);
                    } else {
                        stage.completeExceptionally(throwable1);
                    }
                });
            } else {
                stage.completeExceptionally(throwable);
            }
        });
        return stage;
    }
}
