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
package org.apache.curator.x.async.details;

import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.imps.CuratorFrameworkImpl;
import org.apache.curator.framework.imps.GetConfigBuilderImpl;
import org.apache.curator.x.async.api.AsyncEnsemblable;
import org.apache.curator.x.async.api.AsyncGetConfigBuilder;
import org.apache.curator.x.async.AsyncStage;
import org.apache.zookeeper.data.Stat;

import static org.apache.curator.x.async.details.BackgroundProcs.dataProc;
import static org.apache.curator.x.async.details.BackgroundProcs.safeCall;

class AsyncGetConfigBuilderImpl implements AsyncGetConfigBuilder
{
    private final CuratorFrameworkImpl client;
    private final UnhandledErrorListener unhandledErrorListener;
    private final boolean watched;
    private Stat stat = null;

    AsyncGetConfigBuilderImpl(CuratorFrameworkImpl client, UnhandledErrorListener unhandledErrorListener, boolean watched)
    {
        this.client = client;
        this.unhandledErrorListener = unhandledErrorListener;
        this.watched = watched;
    }

    @Override
    public AsyncEnsemblable<AsyncStage<byte[]>> storingStatIn(Stat stat)
    {
        this.stat = stat;
        return this;
    }

    @Override
    public AsyncStage<byte[]> forEnsemble()
    {
        BuilderCommon<byte[]> common = new BuilderCommon<>(unhandledErrorListener, watched, dataProc);
        GetConfigBuilderImpl builder = new GetConfigBuilderImpl(client, common.backgrounding, common.watcher, stat);
        return safeCall(common.internalCallback, builder::forEnsemble);
    }
}
