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

import org.apache.curator.framework.imps.CuratorFrameworkImpl;
import org.apache.curator.framework.imps.SetDataBuilderImpl;
import org.apache.curator.x.async.AsyncStage;
import org.apache.curator.x.async.api.AsyncPathAndBytesable;
import org.apache.curator.x.async.api.AsyncSetDataBuilder;
import org.apache.zookeeper.data.Stat;

import static org.apache.curator.x.async.details.BackgroundProcs.safeCall;
import static org.apache.curator.x.async.details.BackgroundProcs.statProc;

class AsyncSetDataBuilderImpl implements AsyncSetDataBuilder
{
    private final CuratorFrameworkImpl client;
    private final Filters filters;
    private boolean compressed = false;
    private int version = -1;

    AsyncSetDataBuilderImpl(CuratorFrameworkImpl client, Filters filters)
    {
        this.client = client;
        this.filters = filters;
    }

    @Override
    public AsyncStage<Stat> forPath(String path)
    {
        return internalForPath(path, null, false);
    }

    @Override
    public AsyncStage<Stat> forPath(String path, byte[] data)
    {
        return internalForPath(path, data, true);
    }

    @Override
    public AsyncPathAndBytesable<AsyncStage<Stat>> compressed()
    {
        compressed = true;
        return this;
    }

    @Override
    public AsyncPathAndBytesable<AsyncStage<Stat>> compressedWithVersion(int version)
    {
        compressed = true;
        this.version = version;
        return this;
    }

    @Override
    public AsyncPathAndBytesable<AsyncStage<Stat>> withVersion(int version)
    {
        this.version = version;
        return this;
    }

    private AsyncStage<Stat> internalForPath(String path, byte[] data, boolean useData)
    {
        BuilderCommon<Stat> common = new BuilderCommon<>(filters, statProc);
        SetDataBuilderImpl builder = new SetDataBuilderImpl(client, common.backgrounding, version, compressed);
        return safeCall(common.internalCallback, () -> useData ? builder.forPath(path, data) : builder.forPath(path));
    }
}
