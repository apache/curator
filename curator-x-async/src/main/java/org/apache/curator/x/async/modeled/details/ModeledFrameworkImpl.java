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

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.utils.ThreadUtils;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.AsyncStage;
import org.apache.curator.x.async.WatchMode;
import org.apache.curator.x.async.api.AsyncCuratorFrameworkDsl;
import org.apache.curator.x.async.api.AsyncPathAndBytesable;
import org.apache.curator.x.async.api.AsyncPathable;
import org.apache.curator.x.async.api.AsyncSetDataBuilder;
import org.apache.curator.x.async.api.AsyncTransactionSetDataBuilder;
import org.apache.curator.x.async.api.CreateOption;
import org.apache.curator.x.async.api.WatchableAsyncCuratorFramework;
import org.apache.curator.x.async.modeled.ModelSpec;
import org.apache.curator.x.async.modeled.ModeledFramework;
import org.apache.curator.x.async.modeled.ModeledOptions;
import org.apache.curator.x.async.modeled.ZNode;
import org.apache.curator.x.async.modeled.ZPath;
import org.apache.curator.x.async.modeled.cached.CachedModeledFramework;
import org.apache.curator.x.async.modeled.versioned.VersionedModeledFramework;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

public class ModeledFrameworkImpl<T> implements ModeledFramework<T>
{
    private final AsyncCuratorFramework client;
    private final WatchableAsyncCuratorFramework watchableClient;
    private final ModelSpec<T> modelSpec;
    private final WatchMode watchMode;
    private final UnaryOperator<WatchedEvent> watcherFilter;
    private final UnhandledErrorListener unhandledErrorListener;
    private final UnaryOperator<CuratorEvent> resultFilter;
    private final AsyncCuratorFrameworkDsl dslClient;
    private final boolean isWatched;
    private final Set<ModeledOptions> modeledOptions;

    public static <T> ModeledFrameworkImpl<T> build(AsyncCuratorFramework client, ModelSpec<T> model, WatchMode watchMode, UnaryOperator<WatchedEvent> watcherFilter, UnhandledErrorListener unhandledErrorListener, UnaryOperator<CuratorEvent> resultFilter, Set<ModeledOptions> modeledOptions)
    {
        boolean isWatched = (watchMode != null);

        Objects.requireNonNull(client, "client cannot be null");
        Objects.requireNonNull(model, "model cannot be null");
        modeledOptions = ImmutableSet.copyOf(Objects.requireNonNull(modeledOptions, "modeledOptions cannot be null"));

        watchMode = (watchMode != null) ? watchMode : WatchMode.stateChangeAndSuccess;

        AsyncCuratorFrameworkDsl dslClient = client.with(watchMode, unhandledErrorListener, resultFilter, watcherFilter);
        WatchableAsyncCuratorFramework watchableClient = isWatched ? dslClient.watched() : dslClient;

        return new ModeledFrameworkImpl<>(
            client,
            dslClient,
            watchableClient,
            model,
            watchMode,
            watcherFilter,
            unhandledErrorListener,
            resultFilter,
            isWatched,
            modeledOptions
        );
    }

    private ModeledFrameworkImpl(AsyncCuratorFramework client, AsyncCuratorFrameworkDsl dslClient, WatchableAsyncCuratorFramework watchableClient, ModelSpec<T> modelSpec, WatchMode watchMode, UnaryOperator<WatchedEvent> watcherFilter, UnhandledErrorListener unhandledErrorListener, UnaryOperator<CuratorEvent> resultFilter, boolean isWatched, Set<ModeledOptions> modeledOptions)
    {
        this.client = client;
        this.dslClient = dslClient;
        this.watchableClient = watchableClient;
        this.modelSpec = modelSpec;
        this.watchMode = watchMode;
        this.watcherFilter = watcherFilter;
        this.unhandledErrorListener = unhandledErrorListener;
        this.resultFilter = resultFilter;
        this.isWatched = isWatched;
        this.modeledOptions = modeledOptions;
    }

    @Override
    public CachedModeledFramework<T> cached()
    {
        return cached(ThreadUtils.newSingleThreadExecutor("CachedModeledFramework"));
    }

    @Override
    public CachedModeledFramework<T> cached(ExecutorService executor)
    {
        Preconditions.checkState(!isWatched, "CachedModeledFramework cannot be used with watched instances as the internal cache would bypass the watchers.");
        return new CachedModeledFrameworkImpl<>(this, Objects.requireNonNull(executor, "executor cannot be null"));
    }

    @Override
    public VersionedModeledFramework<T> versioned()
    {
        return new VersionedModeledFrameworkImpl<>(this);
    }

    @Override
    public ModelSpec<T> modelSpec()
    {
        return modelSpec;
    }

    @Override
    public AsyncCuratorFramework unwrap()
    {
        return client;
    }

    @Override
    public AsyncStage<String> set(T item)
    {
        return set(item, null, -1);
    }

    @Override
    public AsyncStage<String> set(T item, Stat storingStatIn)
    {
        return set(item, storingStatIn, -1);
    }

    @Override
    public AsyncStage<String> set(T item, int version)
    {
        return set(item, null, version);
    }

    @Override
    public AsyncStage<String> set(T item, Stat storingStatIn, int version)
    {
        try
        {
            byte[] bytes = modelSpec.serializer().serialize(item);
            return dslClient.create()
                .withOptions(modelSpec.createOptions(), modelSpec.createMode(), fixAclList(modelSpec.aclList()), storingStatIn, modelSpec.ttl(), version)
                .forPath(resolveForSet(item), bytes);
        }
        catch ( Exception e )
        {
            return ModelStage.exceptionally(e);
        }
    }

    @Override
    public AsyncStage<T> read()
    {
        return internalRead(ZNode::model, null);
    }

    @Override
    public AsyncStage<T> read(Stat storingStatIn)
    {
        return internalRead(ZNode::model, storingStatIn);
    }

    @Override
    public AsyncStage<ZNode<T>> readAsZNode()
    {
        return internalRead(Function.identity(), null);
    }

    @Override
    public AsyncStage<Stat> update(T item)
    {
        return update(item, -1);
    }

    @Override
    public AsyncStage<Stat> update(T item, int version)
    {
        try
        {
            byte[] bytes = modelSpec.serializer().serialize(item);
            AsyncSetDataBuilder dataBuilder = dslClient.setData();
            AsyncPathAndBytesable<AsyncStage<Stat>> next = isCompressed() ? dataBuilder.compressedWithVersion(version) : dataBuilder.withVersion(version);
            return next.forPath(resolveForSet(item), bytes);
        }
        catch ( Exception e )
        {
            return ModelStage.exceptionally(e);
        }
    }

    @Override
    public AsyncStage<Stat> checkExists()
    {
        return watchableClient.checkExists().forPath(modelSpec.path().fullPath());
    }

    @Override
    public AsyncStage<Void> delete()
    {
        return delete(-1);
    }

    @Override
    public AsyncStage<Void> delete(int version)
    {
        return dslClient.delete().withVersion(version).forPath(modelSpec.path().fullPath());
    }

    @Override
    public AsyncStage<List<ZPath>> children()
    {
        return internalGetChildren(modelSpec.path());
    }

    @Override
    public AsyncStage<List<ZNode<T>>> childrenAsZNodes()
    {
        ModelStage<List<ZNode<T>>> modelStage = ModelStage.make();
        Preconditions.checkState(!isWatched, "childrenAsZNodes() cannot be used with watched instances.");
        children().handle((children, e) -> {
            if ( e != null )
            {
                modelStage.completeExceptionally(e);
            }
            else
            {
                completeChildrenAsZNodes(modelStage, children);
            }
            return null;
        });
        return modelStage;
    }

    private void completeChildrenAsZNodes(ModelStage<List<ZNode<T>>> modelStage, List<ZPath> children)
    {
        List<ZNode<T>> nodes = Lists.newArrayList();
        if ( children.size() == 0 )
        {
            modelStage.complete(nodes);
            return;
        }
        children.forEach(path -> withPath(path).readAsZNode().handle((node, e) -> {
            if ( e != null )
            {
                modelStage.completeExceptionally(e);
            }
            else
            {
                nodes.add(node);
                if ( nodes.size() == children.size() )
                {
                    modelStage.complete(nodes);
                }
            }
            return null;
        }));
    }

    private AsyncStage<List<ZPath>> internalGetChildren(ZPath path)
    {
        AsyncStage<List<String>> asyncStage = watchableClient.getChildren().forPath(path.fullPath());
        ModelStage<List<ZPath>> modelStage = ModelStage.make(asyncStage.event());
        asyncStage.whenComplete((children, e) -> {
            if ( e != null )
            {
                if ( modeledOptions.contains(ModeledOptions.ignoreMissingNodesForChildren) && (Throwables.getRootCause(e) instanceof KeeperException.NoNodeException) )
                {
                    modelStage.complete(Collections.emptyList());
                }
                else
                {
                    modelStage.completeExceptionally(e);
                }
            }
            else
            {
                modelStage.complete(children.stream().map(path::child).collect(Collectors.toList()));
            }
        });
        return modelStage;
    }

    @Override
    public ModeledFramework<T> parent()
    {
        ModelSpec<T> newModelSpec = modelSpec.parent();
        return new ModeledFrameworkImpl<>(
            client,
            dslClient,
            watchableClient,
            newModelSpec,
            watchMode,
            watcherFilter,
            unhandledErrorListener,
            resultFilter,
            isWatched,
            modeledOptions
        );
    }

    @Override
    public ModeledFramework<T> child(Object child)
    {
        ModelSpec<T> newModelSpec = modelSpec.child(child);
        return new ModeledFrameworkImpl<>(
            client,
            dslClient,
            watchableClient,
            newModelSpec,
            watchMode,
            watcherFilter,
            unhandledErrorListener,
            resultFilter,
            isWatched,
            modeledOptions
        );
    }

    @Override
    public ModeledFramework<T> withPath(ZPath path)
    {
        ModelSpec<T> newModelSpec = modelSpec.withPath(path);
        return new ModeledFrameworkImpl<>(
            client,
            dslClient,
            watchableClient,
            newModelSpec,
            watchMode,
            watcherFilter,
            unhandledErrorListener,
            resultFilter,
            isWatched,
            modeledOptions
        );
    }

    public static boolean isCompressed(Set<CreateOption> createOptions)
    {
        return createOptions.contains(CreateOption.compress);
    }

    @Override
    public CuratorOp createOp(T model)
    {
        return client.transactionOp()
            .create()
            .withOptions(modelSpec.createMode(), fixAclList(modelSpec.aclList()), modelSpec.createOptions().contains(CreateOption.compress), modelSpec.ttl())
            .forPath(resolveForSet(model), modelSpec.serializer().serialize(model));
    }

    @Override
    public CuratorOp updateOp(T model)
    {
        return updateOp(model, -1);
    }

    @Override
    public CuratorOp updateOp(T model, int version)
    {
        AsyncTransactionSetDataBuilder builder = client.transactionOp().setData();
        if ( isCompressed() )
        {
            return builder.withVersionCompressed(version).forPath(resolveForSet(model), modelSpec.serializer().serialize(model));
        }
        return builder.withVersion(version).forPath(resolveForSet(model), modelSpec.serializer().serialize(model));
    }

    @Override
    public CuratorOp deleteOp()
    {
        return deleteOp(-1);
    }

    @Override
    public CuratorOp deleteOp(int version)
    {
        return client.transactionOp().delete().withVersion(version).forPath(modelSpec.path().fullPath());
    }

    @Override
    public CuratorOp checkExistsOp()
    {
        return checkExistsOp(-1);
    }

    @Override
    public CuratorOp checkExistsOp(int version)
    {
        return client.transactionOp().check().withVersion(version).forPath(modelSpec.path().fullPath());
    }

    @Override
    public AsyncStage<List<CuratorTransactionResult>> inTransaction(List<CuratorOp> operations)
    {
        return client.transaction().forOperations(operations);
    }

    private boolean isCompressed()
    {
        return modelSpec.createOptions().contains(CreateOption.compress);
    }

    private <U> ModelStage<U> internalRead(Function<ZNode<T>, U> resolver, Stat storingStatIn)
    {
        Stat stat = (storingStatIn != null) ? storingStatIn : new Stat();
        AsyncPathable<AsyncStage<byte[]>> next = isCompressed() ? watchableClient.getData().decompressedStoringStatIn(stat) : watchableClient.getData().storingStatIn(stat);
        AsyncStage<byte[]> asyncStage = next.forPath(modelSpec.path().fullPath());
        ModelStage<U> modelStage = ModelStage.make(asyncStage.event());
        asyncStage.whenComplete((value, e) -> {
            if ( e != null )
            {
                modelStage.completeExceptionally(e);
            }
            else
            {
                try
                {
                    ZNode<T> node = new ZNodeImpl<>(modelSpec.path(), stat, modelSpec.serializer().deserialize(value));
                    modelStage.complete(resolver.apply(node));
                }
                catch ( Exception deserializeException )
                {
                    modelStage.completeExceptionally(deserializeException);
                }
            }
        });
        return modelStage;
    }

    private String resolveForSet(T model)
    {
        if ( modelSpec.path().isResolved() )
        {
            return modelSpec.path().fullPath();
        }
        return modelSpec.path().resolved(model).fullPath();
    }

    private static List<ACL> fixAclList(List<ACL> aclList)
    {
        return (aclList.size() > 0) ? aclList : null;   // workaround for old, bad design. empty list not accepted
    }
}
