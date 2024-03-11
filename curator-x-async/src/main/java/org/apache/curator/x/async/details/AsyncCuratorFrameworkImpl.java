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

package org.apache.curator.x.async.details;

import static org.apache.curator.x.async.details.BackgroundProcs.*;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Objects;
import java.util.function.UnaryOperator;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.framework.imps.CuratorFrameworkImpl;
import org.apache.curator.framework.imps.CuratorMultiTransactionImpl;
import org.apache.curator.framework.imps.GetACLBuilderImpl;
import org.apache.curator.framework.imps.SyncBuilderImpl;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.AsyncStage;
import org.apache.curator.x.async.WatchMode;
import org.apache.curator.x.async.api.*;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;


public class AsyncCuratorFrameworkImpl implements AsyncCuratorFramework {
    private final CuratorFrameworkImpl client;
    private final Filters filters;
    private final WatchMode watchMode;
    private final boolean watched;

    public AsyncCuratorFrameworkImpl(CuratorFramework client) {
        this(reveal(client), new Filters(null, null, null), WatchMode.stateChangeAndSuccess, false);
    }

    private static CuratorFrameworkImpl reveal(CuratorFramework client) {
        try {
            return (CuratorFrameworkImpl) Objects.requireNonNull(client, "client cannot be null");
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Only Curator clients created through CuratorFrameworkFactory are supported: "
                            + client.getClass().getName());
        }
    }

    public AsyncCuratorFrameworkImpl(
            CuratorFrameworkImpl client, Filters filters, WatchMode watchMode, boolean watched) {
        this.client = Objects.requireNonNull(client, "client cannot be null");
        this.filters = Objects.requireNonNull(filters, "filters cannot be null");
        this.watchMode = Objects.requireNonNull(watchMode, "watchMode cannot be null");
        this.watched = watched;
    }

    @Override
    public AsyncCreateBuilder create() {
        return new AsyncCreateBuilderImpl(client, filters);
    }

    @Override
    public AsyncDeleteBuilder delete() {
        return new AsyncDeleteBuilderImpl(client, filters);
    }

    @Override
    public AsyncSetDataBuilder setData() {
        return new AsyncSetDataBuilderImpl(client, filters);
    }

    @Override
    public AsyncGetACLBuilder getACL() {
        return new AsyncGetACLBuilder() {
            private Stat stat = null;

            @Override
            public AsyncPathable<AsyncStage<List<ACL>>> storingStatIn(Stat stat) {
                this.stat = stat;
                return this;
            }

            @Override
            public AsyncStage<List<ACL>> forPath(String path) {
                BuilderCommon<List<ACL>> common = new BuilderCommon<>(filters, aclProc);
                GetACLBuilderImpl builder = new GetACLBuilderImpl(client, common.backgrounding, stat);
                return safeCall(common.internalCallback, () -> builder.forPath(path));
            }
        };
    }

    @Override
    public AsyncSetACLBuilder setACL() {
        return new AsyncSetACLBuilderImpl(client, filters);
    }

    @Override
    public AsyncReconfigBuilder reconfig() {
        return new AsyncReconfigBuilderImpl(client, filters);
    }

    @Override
    public AsyncMultiTransaction transaction() {
        return operations -> {
            BuilderCommon<List<CuratorTransactionResult>> common = new BuilderCommon<>(filters, opResultsProc);
            CuratorMultiTransactionImpl builder = new CuratorMultiTransactionImpl(client, common.backgrounding);
            return safeCall(common.internalCallback, () -> builder.forOperations(operations));
        };
    }

    @Override
    public AsyncSyncBuilder sync() {
        return path -> {
            BuilderCommon<Void> common = new BuilderCommon<>(filters, ignoredProc);
            SyncBuilderImpl builder = new SyncBuilderImpl(client, common.backgrounding);
            return safeCall(common.internalCallback, () -> builder.forPath(path));
        };
    }

    @Override
    public AsyncRemoveWatchesBuilder removeWatches() {
        return new AsyncRemoveWatchesBuilderImpl(client, filters);
    }

    @Override
    public AsyncWatchBuilder addWatch() {
        Preconditions.checkState(
                client.getZookeeperCompatibility().hasPersistentWatchers(),
                "addWatch() is not supported in the ZooKeeper library and/or server being used.");
        return new AsyncWatchBuilderImpl(client, filters);
    }

    @Override
    public CuratorFramework unwrap() {
        return client;
    }

    @Override
    public WatchableAsyncCuratorFramework watched() {
        return new AsyncCuratorFrameworkImpl(client, filters, watchMode, true);
    }

    @Override
    public AsyncCuratorFrameworkDsl with(WatchMode mode) {
        return new AsyncCuratorFrameworkImpl(client, filters, mode, watched);
    }

    @Override
    public AsyncCuratorFrameworkDsl with(
            WatchMode mode,
            UnhandledErrorListener listener,
            UnaryOperator<CuratorEvent> resultFilter,
            UnaryOperator<WatchedEvent> watcherFilter) {
        return new AsyncCuratorFrameworkImpl(
                client, new Filters(listener, filters.getResultFilter(), filters.getWatcherFilter()), mode, watched);
    }

    @Override
    public AsyncCuratorFrameworkDsl with(UnhandledErrorListener listener) {
        return new AsyncCuratorFrameworkImpl(
                client,
                new Filters(listener, filters.getResultFilter(), filters.getWatcherFilter()),
                watchMode,
                watched);
    }

    @Override
    public AsyncCuratorFrameworkDsl with(
            UnaryOperator<CuratorEvent> resultFilter, UnaryOperator<WatchedEvent> watcherFilter) {
        return new AsyncCuratorFrameworkImpl(
                client, new Filters(filters.getListener(), resultFilter, watcherFilter), watchMode, watched);
    }

    @Override
    public AsyncCuratorFrameworkDsl with(
            UnhandledErrorListener listener,
            UnaryOperator<CuratorEvent> resultFilter,
            UnaryOperator<WatchedEvent> watcherFilter) {
        return new AsyncCuratorFrameworkImpl(
                client, new Filters(listener, resultFilter, watcherFilter), watchMode, watched);
    }

    @Override
    public AsyncTransactionOp transactionOp() {
        return new AsyncTransactionOpImpl(client);
    }

    @Override
    public AsyncExistsBuilder checkExists() {
        return new AsyncExistsBuilderImpl(client, filters, getBuilderWatchMode());
    }

    @Override
    public AsyncGetDataBuilder getData() {
        return new AsyncGetDataBuilderImpl(client, filters, getBuilderWatchMode());
    }

    @Override
    public AsyncGetChildrenBuilder getChildren() {
        return new AsyncGetChildrenBuilderImpl(client, filters, getBuilderWatchMode());
    }

    @Override
    public AsyncGetConfigBuilder getConfig() {
        return new AsyncGetConfigBuilderImpl(client, filters, getBuilderWatchMode());
    }

    Filters getFilters() {
        return filters;
    }

    CuratorFrameworkImpl getClient() {
        return client;
    }

    private WatchMode getBuilderWatchMode() {
        return watched ? watchMode : null;
    }
}
