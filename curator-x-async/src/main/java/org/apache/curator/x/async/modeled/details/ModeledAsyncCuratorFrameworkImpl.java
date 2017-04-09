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

import com.google.common.collect.ImmutableSet;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.utils.ZKPaths;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.AsyncStage;
import org.apache.curator.x.async.WatchMode;
import org.apache.curator.x.async.api.AsyncCuratorFrameworkDsl;
import org.apache.curator.x.async.api.AsyncPathAndBytesable;
import org.apache.curator.x.async.api.AsyncPathable;
import org.apache.curator.x.async.api.CreateOption;
import org.apache.curator.x.async.api.DeleteOption;
import org.apache.curator.x.async.api.WatchableAsyncCuratorFramework;
import org.apache.curator.x.async.modeled.ModelSerializer;
import org.apache.curator.x.async.modeled.ModeledAsyncCuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.UnaryOperator;

public class ModeledAsyncCuratorFrameworkImpl<T> implements ModeledAsyncCuratorFramework<T>
{
    private final AsyncCuratorFramework client;
    private final WatchableAsyncCuratorFramework watchableClient;
    private final String path;
    private final ModelSerializer<T> serializer;
    private final WatchMode watchMode;
    private final UnaryOperator<WatchedEvent> watcherFilter;
    private final UnhandledErrorListener unhandledErrorListener;
    private final UnaryOperator<CuratorEvent> resultFilter;
    private final CreateMode createMode;
    private final List<ACL> aclList;
    private final Set<CreateOption> createOptions;
    private final Set<DeleteOption> deleteOptions;
    private final AsyncCuratorFrameworkDsl dslClient;

    public ModeledAsyncCuratorFrameworkImpl(CuratorFramework client, String path, ModelSerializer<T> serializer, WatchMode watchMode, UnaryOperator<WatchedEvent> watcherFilter, UnhandledErrorListener unhandledErrorListener, UnaryOperator<CuratorEvent> resultFilter, CreateMode createMode, List<ACL> aclList, Set<CreateOption> createOptions, Set<DeleteOption> deleteOptions)
    {
        boolean localIsWatched = (watchMode != null);

        this.client = AsyncCuratorFramework.wrap(client);
        this.path = Objects.requireNonNull(path, "path cannot be null");
        this.serializer = Objects.requireNonNull(serializer, "serializer cannot be null");
        this.watchMode = (watchMode != null) ? watchMode : WatchMode.stateChangeAndSuccess;
        this.watcherFilter = watcherFilter;
        this.unhandledErrorListener = unhandledErrorListener;
        this.resultFilter = resultFilter;
        this.createMode = (createMode != null) ? createMode : CreateMode.PERSISTENT;
        this.aclList = aclList;
        this.createOptions = (createOptions != null) ? ImmutableSet.copyOf(createOptions) : Collections.emptySet();
        this.deleteOptions = (deleteOptions != null) ? ImmutableSet.copyOf(deleteOptions) : Collections.emptySet();

        dslClient = this.client.with(this.watchMode, unhandledErrorListener, resultFilter, watcherFilter);
        watchableClient = localIsWatched ? dslClient.watched() : dslClient;
    }

    @Override
    public CuratorFramework unwrap()
    {
        return client.unwrap();
    }

    @Override
    public AsyncStage<String> create(T model)
    {
        return create(model, null);
    }

    @Override
    public AsyncStage<String> create(T model, Stat storingStatIn)
    {
        byte[] bytes = serializer.serialize(model);
        return dslClient.create().withOptions(createOptions, createMode, aclList, storingStatIn).forPath(path, bytes);
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
        AsyncStage<byte[]> asyncStage = next.forPath(path);
        ModelStage<T> modelStage = new ModelStage<T>()
        {
            @Override
            public CompletionStage<WatchedEvent> event()
            {
                return asyncStage.event();
            }
        };
        asyncStage.whenComplete((value, e) -> {
            if ( e != null )
            {
                modelStage.completeExceptionally(e);
            }
            else
            {
                modelStage.complete(serializer.deserialize(value));
            }
        });
        return modelStage;
    }

    @Override
    public AsyncStage<Stat> update(T model)
    {
        return update(model, -1);
    }

    @Override
    public AsyncStage<Stat> update(T model, int version)
    {
        byte[] bytes = serializer.serialize(model);
        AsyncPathAndBytesable<AsyncStage<Stat>> next = isCompressed() ? dslClient.setData().compressedWithVersion(version) : dslClient.setData();
        return next.forPath(path, bytes);
    }

    @Override
    public AsyncStage<Stat> checkExists()
    {
        return watchableClient.checkExists().forPath(path);
    }

    @Override
    public AsyncStage<Void> delete()
    {
        return delete(-1);
    }

    @Override
    public AsyncStage<Void> delete(int version)
    {
        return dslClient.delete().withVersion(-1).forPath(path);
    }

    @Override
    public ModeledAsyncCuratorFramework<T> at(String child)
    {
        String childPath = ZKPaths.makePath(path, child);
        return new ModeledAsyncCuratorFrameworkImpl<>(client.unwrap(), childPath, serializer, watchMode, watcherFilter, unhandledErrorListener, resultFilter, createMode, aclList, createOptions, deleteOptions);
    }

    private boolean isCompressed()
    {
        return createOptions.contains(CreateOption.compress);
    }
}
