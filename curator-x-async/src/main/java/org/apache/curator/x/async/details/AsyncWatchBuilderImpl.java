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
import org.apache.curator.framework.api.WatchableBase;
import org.apache.curator.framework.imps.AddWatchBuilderImpl;
import org.apache.curator.framework.imps.CuratorFrameworkImpl;
import org.apache.curator.framework.imps.Watching;
import org.apache.curator.x.async.AsyncStage;
import org.apache.curator.x.async.api.AsyncPathable;
import org.apache.curator.x.async.api.AsyncWatchBuilder;
import org.apache.curator.x.async.api.AsyncWatchBuilder2;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.Watcher;

import static org.apache.curator.x.async.details.BackgroundProcs.ignoredProc;
import static org.apache.curator.x.async.details.BackgroundProcs.safeCall;

class AsyncWatchBuilderImpl implements AsyncWatchBuilder, AsyncWatchBuilder2, WatchableBase<AsyncPathable<AsyncStage<Void>>>, AsyncPathable<AsyncStage<Void>>
{
    private final CuratorFrameworkImpl client;
    private final Filters filters;
    private Watching watching;
    private AddWatchMode mode = AddWatchMode.PERSISTENT_RECURSIVE;

    AsyncWatchBuilderImpl(CuratorFrameworkImpl client, Filters filters)
    {
        this.client = client;
        this.filters = filters;
        watching = new Watching(client, true);
    }

    @Override
    public AsyncWatchBuilder2 withMode(AddWatchMode mode)
    {
        this.mode = mode;
        return this;
    }

    @Override
    public AsyncPathable<AsyncStage<Void>> usingWatcher(Watcher watcher)
    {
        watching = new Watching(client, watcher);
        return this;
    }

    @Override
    public AsyncPathable<AsyncStage<Void>> usingWatcher(CuratorWatcher watcher)
    {
        watching = new Watching(client, watcher);
        return this;
    }

    @Override
    public AsyncStage<Void> forPath(String path)
    {
        BuilderCommon<Void> common = new BuilderCommon<>(filters, ignoredProc);
        AddWatchBuilderImpl builder = new AddWatchBuilderImpl(client, watching, common.backgrounding, mode);
        return safeCall(common.internalCallback, () -> builder.forPath(path));
    }
}