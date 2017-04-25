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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.AsyncStage;
import org.apache.curator.x.async.WatchMode;
import org.apache.curator.x.async.api.AsyncCuratorFrameworkDsl;
import org.apache.curator.x.async.api.AsyncPathAndBytesable;
import org.apache.curator.x.async.api.AsyncPathable;
import org.apache.curator.x.async.api.CreateOption;
import org.apache.curator.x.async.api.WatchableAsyncCuratorFramework;
import org.apache.curator.x.async.modeled.CuratorModelSpec;
import org.apache.curator.x.async.modeled.ModeledCuratorFramework;
import org.apache.curator.x.async.modeled.ZPath;
import org.apache.curator.x.async.modeled.caching.Caching;
import org.apache.curator.x.async.modeled.caching.CachingOption;
import org.apache.curator.x.async.modeled.recipes.ModeledCachedNode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.DataTree;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

public class ModeledCuratorFrameworkImpl<T> implements ModeledCuratorFramework<T>
{
    private final AsyncCuratorFramework client;
    private final WatchableAsyncCuratorFramework watchableClient;
    private final CuratorModelSpec<T> model;
    private final WatchMode watchMode;
    private final UnaryOperator<WatchedEvent> watcherFilter;
    private final UnhandledErrorListener unhandledErrorListener;
    private final UnaryOperator<CuratorEvent> resultFilter;
    private final AsyncCuratorFrameworkDsl dslClient;
    private final CachingImpl<T> caching;

    public static <T> ModeledCuratorFrameworkImpl<T> build(CuratorFramework client, CuratorModelSpec<T> model, WatchMode watchMode, UnaryOperator<WatchedEvent> watcherFilter, UnhandledErrorListener unhandledErrorListener, UnaryOperator<CuratorEvent> resultFilter, Set<CachingOption> cachingOptions, boolean cached)
    {
        boolean localIsWatched = (watchMode != null);

        Objects.requireNonNull(client, "client cannot be null");
        Objects.requireNonNull(model, "model cannot be null");

        watchMode = (watchMode != null) ? watchMode : WatchMode.stateChangeAndSuccess;

        AsyncCuratorFramework asyncClient = AsyncCuratorFramework.wrap(client);
        AsyncCuratorFrameworkDsl dslClient = asyncClient.with(watchMode, unhandledErrorListener, resultFilter, watcherFilter);
        WatchableAsyncCuratorFramework watchableClient = localIsWatched ? dslClient.watched() : dslClient;

        CachingImpl<T> caching = cached ? new CachingImpl<>(client, model.serializer(), model.path(), cachingOptions, model.createOptions()) : null;

        return new ModeledCuratorFrameworkImpl<>(
            asyncClient,
            dslClient,
            watchableClient,
            model,
            watchMode,
            watcherFilter,
            unhandledErrorListener,
            resultFilter,
            caching
        );
    }

    private ModeledCuratorFrameworkImpl(AsyncCuratorFramework client, AsyncCuratorFrameworkDsl dslClient, WatchableAsyncCuratorFramework watchableClient, CuratorModelSpec<T> model, WatchMode watchMode, UnaryOperator<WatchedEvent> watcherFilter, UnhandledErrorListener unhandledErrorListener, UnaryOperator<CuratorEvent> resultFilter, CachingImpl<T> caching)
    {
        this.client = client;
        this.dslClient = dslClient;
        this.watchableClient = watchableClient;
        this.model = model;
        this.watchMode = watchMode;
        this.watcherFilter = watcherFilter;
        this.unhandledErrorListener = unhandledErrorListener;
        this.resultFilter = resultFilter;
        this.caching = caching;
    }

    @Override
    public CuratorFramework unwrap()
    {
        return client.unwrap();
    }

    @Override
    public Caching<T> caching()
    {
        Preconditions.checkState(caching != null, "Caching is not enabled for this instance");
        return caching;
    }

    @Override
    public AsyncStage<String> create(T item)
    {
        return create(item, null);
    }

    @Override
    public AsyncStage<String> create(T item, Stat storingStatIn)
    {
        long dirtyZxid = getDirtyZxid();
        byte[] bytes = model.serializer().serialize(item);
        AsyncStage<String> asyncStage = dslClient.create().withOptions(model.createOptions(), model.createMode(), fixAclList(model.aclList()), storingStatIn).forPath(model.path().fullPath(), bytes);
        ModelStage<String> modelStage = new ModelStage<>(asyncStage.event());
        markDirtyCompleter(dirtyZxid, asyncStage, modelStage);
        return modelStage;
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

    @VisibleForTesting
    volatile AtomicInteger debugCachedReadCount = null;

    @Override
    public AsyncStage<T> read(Stat storingStatIn)
    {
        ModeledCachedNode<T> node = getCached();
        if ( node != null )
        {
            if ( node.getModel() != null )
            {
                if ( storingStatIn != null )
                {
                    DataTree.copyStat(node.getStat(), storingStatIn);
                }
                if ( debugCachedReadCount != null )
                {
                    debugCachedReadCount.incrementAndGet();
                }
                return new ModelStage<>(node.getModel());
            }
        }

        AsyncPathable<AsyncStage<byte[]>> next;
        if ( isCompressed() )
        {
            next = (storingStatIn != null) ? watchableClient.getData().decompressedStoringStatIn(storingStatIn) : watchableClient.getData().decompressed();
        }
        else
        {
            next = (storingStatIn != null) ? watchableClient.getData().storingStatIn(storingStatIn) : watchableClient.getData();
        }
        AsyncStage<byte[]> asyncStage = next.forPath(model.path().fullPath());
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
                    modelStage.complete(model.serializer().deserialize(value));
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
        long dirtyZxid = getDirtyZxid();
        byte[] bytes = model.serializer().serialize(item);
        AsyncPathAndBytesable<AsyncStage<Stat>> next = isCompressed() ? dslClient.setData().compressedWithVersion(version) : dslClient.setData();
        AsyncStage<Stat> asyncStage = next.forPath(model.path().fullPath(), bytes);
        ModelStage<Stat> modelStage = new ModelStage<>(asyncStage.event());
        markDirtyCompleter(dirtyZxid, asyncStage, modelStage);
        return modelStage;
    }

    @Override
    public AsyncStage<Stat> checkExists()
    {
        ModeledCachedNode<T> node = getCached();
        if ( node != null )
        {
            AsyncStage<Stat> result = new ModelStage<>(node.getStat());
            if ( debugCachedReadCount != null )
            {
                debugCachedReadCount.incrementAndGet();
            }
            return result;
        }
        return watchableClient.checkExists().forPath(model.path().fullPath());
    }

    @Override
    public AsyncStage<Void> delete()
    {
        return delete(-1);
    }

    @Override
    public AsyncStage<Void> delete(int version)
    {
        long dirtyZxid = getDirtyZxid();
        AsyncStage<Void> asyncStage = dslClient.delete().withVersion(-1).forPath(model.path().fullPath());
        ModelStage<Void> modelStage = new ModelStage<>(asyncStage.event());
        markDirtyCompleter(dirtyZxid, asyncStage, modelStage);
        return modelStage;
    }

    @Override
    public AsyncStage<List<ZPath>> getChildren()
    {
        AsyncStage<List<String>> asyncStage = watchableClient.getChildren().forPath(model.path().fullPath());
        ModelStage<List<ZPath>> modelStage = new ModelStage<>(asyncStage.event());
        asyncStage.whenComplete((children, e) -> {
            if ( e != null )
            {
                modelStage.completeExceptionally(e);
            }
            else
            {
                modelStage.complete(children.stream().map(child -> model.path().at(child)).collect(Collectors.toList()));
            }
        });
        return modelStage;
    }

    @Override
    public ModeledCuratorFramework<T> at(String child)
    {
        CuratorModelSpec<T> childModel = model.at(child);
        CachingImpl<T> newCaching = (caching != null) ? caching.at(child) : null;
        return new ModeledCuratorFrameworkImpl<>(
            client,
            dslClient,
            watchableClient,
            childModel,
            watchMode,
            watcherFilter,
            unhandledErrorListener,
            resultFilter,
            newCaching
        );
    }

    public static boolean isCompressed(Set<CreateOption> createOptions)
    {
        return createOptions.contains(CreateOption.compress);
    }

    private <U> void markDirtyCompleter(long dirtyZxid, AsyncStage<U> asyncStage, ModelStage<U> modelStage)
    {
        asyncStage.whenComplete((value, e) -> {
            if ( e != null )
            {
                modelStage.completeExceptionally(e);
            }
            else
            {
                if ( caching != null )
                {
                    caching.markDirty(dirtyZxid);
                }
                modelStage.complete(value);
            }
        });
    }

    private boolean isCompressed()
    {
        return model.createOptions().contains(CreateOption.compress);
    }

    private ModeledCachedNode<T> getCached()
    {
        return (caching != null) ? caching.getCacheIf() : null;
    }

    private long getDirtyZxid()
    {
        return (caching != null) ? caching.getCurrentZxid() : -1L;
    }
}
