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

import static org.apache.curator.x.async.details.BackgroundProcs.dataProc;
import static org.apache.curator.x.async.details.BackgroundProcs.safeCall;
import org.apache.curator.framework.imps.GetDataBuilderImpl;
import org.apache.curator.framework.imps.InternalCuratorFramework;
import org.apache.curator.x.async.AsyncStage;
import org.apache.curator.x.async.WatchMode;
import org.apache.curator.x.async.api.AsyncGetDataBuilder;
import org.apache.curator.x.async.api.AsyncPathable;
import org.apache.zookeeper.data.Stat;

class AsyncGetDataBuilderImpl implements AsyncGetDataBuilder {
    private final InternalCuratorFramework client;
    private final Filters filters;
    private final WatchMode watchMode;
    private boolean decompressed;
    private Stat stat = null;

    AsyncGetDataBuilderImpl(InternalCuratorFramework client, Filters filters, WatchMode watchMode) {
        this.client = client;
        this.filters = filters;
        this.watchMode = watchMode;
        this.decompressed = client.compressionEnabled();
    }

    @Override
    public AsyncPathable<AsyncStage<byte[]>> decompressed() {
        decompressed = true;
        return this;
    }

    @Override
    public AsyncPathable<AsyncStage<byte[]>> undecompressed() {
        decompressed = false;
        return this;
    }

    @Override
    public AsyncPathable<AsyncStage<byte[]>> storingStatIn(Stat stat) {
        this.stat = stat;
        return this;
    }

    @Override
    public AsyncPathable<AsyncStage<byte[]>> decompressedStoringStatIn(Stat stat) {
        decompressed = true;
        this.stat = stat;
        return this;
    }

    @Override
    public AsyncPathable<AsyncStage<byte[]>> undecompressedStoringStatIn(Stat stat) {
        decompressed = false;
        this.stat = stat;
        return this;
    }

    @Override
    public AsyncStage<byte[]> forPath(String path) {
        BuilderCommon<byte[]> common = new BuilderCommon<>(filters, watchMode, dataProc);
        GetDataBuilderImpl builder =
                new GetDataBuilderImpl(client, stat, common.watcher, common.backgrounding, decompressed);
        return safeCall(common.internalCallback, () -> builder.forPath(path));
    }
}
