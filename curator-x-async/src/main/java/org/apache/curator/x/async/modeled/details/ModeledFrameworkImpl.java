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
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.AsyncStage;
import org.apache.curator.x.async.WatchMode;
import org.apache.curator.x.async.api.AsyncCuratorFrameworkDsl;
import org.apache.curator.x.async.api.AsyncPathAndBytesable;
import org.apache.curator.x.async.api.AsyncPathable;
import org.apache.curator.x.async.api.AsyncTransactionSetDataBuilder;
import org.apache.curator.x.async.api.CreateOption;
import org.apache.curator.x.async.api.WatchableAsyncCuratorFramework;
import org.apache.curator.x.async.modeled.ModelSpec;
import org.apache.curator.x.async.modeled.ModeledFramework;
import org.apache.curator.x.async.modeled.ZPath;
import org.apache.curator.x.async.modeled.cached.CachedModeledFramework;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import java.util.List;
import java.util.Objects;
import java.util.Set;
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

    public static <T> ModeledFrameworkImpl<T> build(CuratorFramework client, ModelSpec<T> model, WatchMode watchMode, UnaryOperator<WatchedEvent> watcherFilter, UnhandledErrorListener unhandledErrorListener, UnaryOperator<CuratorEvent> resultFilter)
    {
        boolean localIsWatched = (watchMode != null);

        Objects.requireNonNull(client, "client cannot be null");
        Objects.requireNonNull(model, "model cannot be null");

        watchMode = (watchMode != null) ? watchMode : WatchMode.stateChangeAndSuccess;

        AsyncCuratorFramework asyncClient = AsyncCuratorFramework.wrap(client);
        AsyncCuratorFrameworkDsl dslClient = asyncClient.with(watchMode, unhandledErrorListener, resultFilter, watcherFilter);
        WatchableAsyncCuratorFramework watchableClient = localIsWatched ? dslClient.watched() : dslClient;

        return new ModeledFrameworkImpl<>(
            asyncClient,
            dslClient,
            watchableClient,
            model,
            watchMode,
            watcherFilter,
            unhandledErrorListener,
            resultFilter
        );
    }

    private ModeledFrameworkImpl(AsyncCuratorFramework client, AsyncCuratorFrameworkDsl dslClient, WatchableAsyncCuratorFramework watchableClient, ModelSpec<T> modelSpec, WatchMode watchMode, UnaryOperator<WatchedEvent> watcherFilter, UnhandledErrorListener unhandledErrorListener, UnaryOperator<CuratorEvent> resultFilter)
    {
        this.client = client;
        this.dslClient = dslClient;
        this.watchableClient = watchableClient;
        this.modelSpec = modelSpec;
        this.watchMode = watchMode;
        this.watcherFilter = watcherFilter;
        this.unhandledErrorListener = unhandledErrorListener;
        this.resultFilter = resultFilter;
    }

    @Override
    public CachedModeledFramework<T> cached()
    {
        return new CachedModeledFrameworkImpl<>(this);
    }

    @Override
    public ModelSpec<T> modelSpec()
    {
        return modelSpec;
    }

    @Override
    public CuratorFramework unwrap()
    {
        return client.unwrap();
    }

    @Override
    public AsyncStage<String> set(T item)
    {
        return set(item, null);
    }

    @Override
    public AsyncStage<String> set(T item, Stat storingStatIn)
    {
        byte[] bytes = modelSpec.serializer().serialize(item);
        return dslClient.create().withOptions(modelSpec.createOptions(), modelSpec.createMode(), fixAclList(modelSpec.aclList()), storingStatIn).forPath(modelSpec.path().fullPath(), bytes);
    }

    private List<ACL> fixAclList(List<ACL> aclList)
    {
        return (aclList.size() > 0) ? aclList : null;   // workaround for old, bad design. empty list not accepted
    }

    @Override
    public AsyncStage<T> read()
    {
        return read(null);
    }

    @Override
    public AsyncStage<T> read(Stat storingStatIn)
    {
        AsyncPathable<AsyncStage<byte[]>> next;
        if ( isCompressed() )
        {
            next = (storingStatIn != null) ? watchableClient.getData().decompressedStoringStatIn(storingStatIn) : watchableClient.getData().decompressed();
        }
        else
        {
            next = (storingStatIn != null) ? watchableClient.getData().storingStatIn(storingStatIn) : watchableClient.getData();
        }
        AsyncStage<byte[]> asyncStage = next.forPath(modelSpec.path().fullPath());
        ModelStage<T> modelStage = new ModelStage<>(asyncStage.event());
        asyncStage.whenComplete((value, e) -> {
            if ( e != null )
            {
                modelStage.completeExceptionally(e);
            }
            else
            {
                try
                {
                    modelStage.complete(modelSpec.serializer().deserialize(value));
                }
                catch ( Exception deserializeException )
                {
                    modelStage.completeExceptionally(deserializeException);
                }
            }
        });
        return modelStage;
    }

    @Override
    public AsyncStage<Stat> update(T item)
    {
        return update(item, -1);
    }

    @Override
    public AsyncStage<Stat> update(T item, int version)
    {
        byte[] bytes = modelSpec.serializer().serialize(item);
        AsyncPathAndBytesable<AsyncStage<Stat>> next = isCompressed() ? dslClient.setData().compressedWithVersion(version) : dslClient.setData();
        return next.forPath(modelSpec.path().fullPath(), bytes);
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
        return dslClient.delete().withVersion(-1).forPath(modelSpec.path().fullPath());
    }

    @Override
    public AsyncStage<List<ZPath>> children()
    {
        AsyncStage<List<String>> asyncStage = watchableClient.getChildren().forPath(modelSpec.path().fullPath());
        ModelStage<List<ZPath>> modelStage = new ModelStage<>(asyncStage.event());
        asyncStage.whenComplete((children, e) -> {
            if ( e != null )
            {
                modelStage.completeExceptionally(e);
            }
            else
            {
                modelStage.complete(children.stream().map(child -> modelSpec.path().at(child)).collect(Collectors.toList()));
            }
        });
        return modelStage;
    }

    @Override
    public ModeledFramework<T> at(String child)
    {
        ModelSpec<T> newModelSpec = modelSpec.at(child);
        return new ModeledFrameworkImpl<>(
            client,
            dslClient,
            watchableClient,
            newModelSpec,
            watchMode,
            watcherFilter,
            unhandledErrorListener,
            resultFilter
        );
    }

    @Override
    public ModeledFramework<T> at(ZPath path)
    {
        ModelSpec<T> newModelSpec = modelSpec.at(path);
        return new ModeledFrameworkImpl<>(
            client,
            dslClient,
            watchableClient,
            newModelSpec,
            watchMode,
            watcherFilter,
            unhandledErrorListener,
            resultFilter
        );
    }

    @Override
    public ModeledFramework<T> resolved(T model)
    {
        ModelSpec<T> newModelSpec = modelSpec.resolved(model);
        return new ModeledFrameworkImpl<>(
            client,
            dslClient,
            watchableClient,
            newModelSpec,
            watchMode,
            watcherFilter,
            unhandledErrorListener,
            resultFilter
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
            .withOptions(modelSpec.createMode(), modelSpec.aclList(), modelSpec.createOptions().contains(CreateOption.compress))
            .forPath(modelSpec.path().fullPath(), modelSpec.serializer().serialize(model));
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
            return builder.withVersionCompressed(version).forPath(modelSpec.path().fullPath(), modelSpec.serializer().serialize(model));
        }
        return builder.withVersion(version).forPath(modelSpec.path().fullPath(), modelSpec.serializer().serialize(model));
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
}
