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

import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.imps.CuratorFrameworkImpl;
import org.apache.curator.framework.imps.RemoveWatchesBuilderImpl;
import org.apache.curator.x.async.AsyncStage;
import org.apache.curator.x.async.api.AsyncPathable;
import org.apache.curator.x.async.api.AsyncRemoveWatchesBuilder;
import org.apache.curator.x.async.api.RemoveWatcherOption;
import org.apache.zookeeper.Watcher;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import static org.apache.curator.x.async.details.BackgroundProcs.ignoredProc;
import static org.apache.curator.x.async.details.BackgroundProcs.safeCall;

class AsyncRemoveWatchesBuilderImpl implements AsyncRemoveWatchesBuilder, AsyncPathable<AsyncStage<Void>>
{
    private final CuratorFrameworkImpl client;
    private final Filters filters;
    private Watcher.WatcherType watcherType = Watcher.WatcherType.Any;
    private Set<RemoveWatcherOption> options = Collections.emptySet();
    private Watcher watcher = null;
    private CuratorWatcher curatorWatcher = null;

    AsyncRemoveWatchesBuilderImpl(CuratorFrameworkImpl client, Filters filters)
    {
        this.client = client;
        this.filters = filters;
    }

    @Override
    public AsyncPathable<AsyncStage<Void>> removing(Watcher watcher)
    {
        this.watcher = Objects.requireNonNull(watcher, "watcher cannot be null");
        this.curatorWatcher = null;
        return this;
    }

    @Override
    public AsyncPathable<AsyncStage<Void>> removing(CuratorWatcher watcher)
    {
        this.curatorWatcher = Objects.requireNonNull(watcher, "watcher cannot be null");
        this.watcher = null;
        return this;
    }

    @Override
    public AsyncPathable<AsyncStage<Void>> removingAll()
    {
        this.curatorWatcher = null;
        this.watcher = null;
        return this;
    }

    @Override
    public AsyncPathable<AsyncStage<Void>> removing(Watcher watcher, Set<RemoveWatcherOption> options)
    {
        this.watcher = Objects.requireNonNull(watcher, "watcher cannot be null");
        this.options = Objects.requireNonNull(options, "options cannot be null");
        this.curatorWatcher = null;
        return this;
    }

    @Override
    public AsyncPathable<AsyncStage<Void>> removing(CuratorWatcher watcher, Set<RemoveWatcherOption> options)
    {
        this.curatorWatcher = Objects.requireNonNull(watcher, "watcher cannot be null");
        this.options = Objects.requireNonNull(options, "options cannot be null");
        this.watcher = null;
        return this;
    }

    @Override
    public AsyncPathable<AsyncStage<Void>> removingAll(Set<RemoveWatcherOption> options)
    {
        this.options = Objects.requireNonNull(options, "options cannot be null");
        this.curatorWatcher = null;
        this.watcher = null;
        return this;
    }

    @Override
    public AsyncPathable<AsyncStage<Void>> removing(Watcher watcher, Watcher.WatcherType watcherType, Set<RemoveWatcherOption> options)
    {
        this.watcher = Objects.requireNonNull(watcher, "watcher cannot be null");
        this.options = Objects.requireNonNull(options, "options cannot be null");
        this.watcherType = Objects.requireNonNull(watcherType, "watcherType cannot be null");
        this.curatorWatcher = null;
        return this;
    }

    @Override
    public AsyncPathable<AsyncStage<Void>> removing(CuratorWatcher watcher, Watcher.WatcherType watcherType, Set<RemoveWatcherOption> options)
    {
        this.curatorWatcher = Objects.requireNonNull(watcher, "watcher cannot be null");
        this.options = Objects.requireNonNull(options, "options cannot be null");
        this.watcherType = Objects.requireNonNull(watcherType, "watcherType cannot be null");
        this.watcher = null;
        return this;
    }

    @Override
    public AsyncPathable<AsyncStage<Void>> removingAll(Watcher.WatcherType watcherType, Set<RemoveWatcherOption> options)
    {
        this.options = Objects.requireNonNull(options, "options cannot be null");
        this.watcherType = Objects.requireNonNull(watcherType, "watcherType cannot be null");
        this.curatorWatcher = null;
        this.watcher = null;
        return this;
    }

    @Override
    public AsyncPathable<AsyncStage<Void>> removing(Watcher watcher, Watcher.WatcherType watcherType)
    {
        this.watcher = Objects.requireNonNull(watcher, "watcher cannot be null");
        this.watcherType = Objects.requireNonNull(watcherType, "watcherType cannot be null");
        this.curatorWatcher = null;
        return this;
    }

    @Override
    public AsyncPathable<AsyncStage<Void>> removing(CuratorWatcher watcher, Watcher.WatcherType watcherType)
    {
        this.curatorWatcher = Objects.requireNonNull(watcher, "watcher cannot be null");
        this.watcherType = Objects.requireNonNull(watcherType, "watcherType cannot be null");
        this.watcher = null;
        return this;
    }

    @Override
    public AsyncPathable<AsyncStage<Void>> removingAll(Watcher.WatcherType watcherType)
    {
        this.watcherType = Objects.requireNonNull(watcherType, "watcherType cannot be null");
        this.curatorWatcher = null;
        this.watcher = null;
        return this;
    }

    @Override
    public AsyncStage<Void> forPath(String path)
    {
        BuilderCommon<Void> common = new BuilderCommon<>(filters, ignoredProc);
        RemoveWatchesBuilderImpl builder = new RemoveWatchesBuilderImpl(client,
            watcher,
            curatorWatcher,
            watcherType,
            options.contains(RemoveWatcherOption.guaranteed),
            options.contains(RemoveWatcherOption.local),
            options.contains(RemoveWatcherOption.guaranteed),
            common.backgrounding
        );
        return safeCall(common.internalCallback, () -> builder.forPath(path));
    }
}
