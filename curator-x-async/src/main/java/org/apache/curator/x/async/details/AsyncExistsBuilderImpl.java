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
import org.apache.curator.framework.imps.ExistsBuilderImpl;
import org.apache.curator.x.async.AsyncExistsBuilder;
import org.apache.curator.x.async.AsyncPathable;
import org.apache.curator.x.async.AsyncStage;
import org.apache.zookeeper.data.Stat;

import static org.apache.curator.x.async.details.BackgroundProcs.safeCall;
import static org.apache.curator.x.async.details.BackgroundProcs.safeStatProc;

class AsyncExistsBuilderImpl implements AsyncExistsBuilder
{
    private final CuratorFrameworkImpl client;
    private final UnhandledErrorListener unhandledErrorListener;
    private final boolean watched;
    private boolean createParentsIfNeeded = false;
    private boolean createParentContainersIfNeeded = false;

    AsyncExistsBuilderImpl(CuratorFrameworkImpl client, UnhandledErrorListener unhandledErrorListener, boolean watched)
    {
        this.client = client;
        this.unhandledErrorListener = unhandledErrorListener;
        this.watched = watched;
    }

    @Override
    public AsyncPathable<AsyncStage<Stat>> creatingParentsIfNeeded()
    {
        createParentsIfNeeded = true;
        return this;
    }

    @Override
    public AsyncPathable<AsyncStage<Stat>> creatingParentContainersIfNeeded()
    {
        createParentContainersIfNeeded = true;
        return this;
    }

    @Override
    public AsyncStage<Stat> forPath(String path)
    {
        BuilderCommon<Stat> common = new BuilderCommon<>(unhandledErrorListener, watched, safeStatProc);
        ExistsBuilderImpl builder = new ExistsBuilderImpl(client, common.backgrounding, common.watcher, createParentsIfNeeded, createParentContainersIfNeeded);
        return safeCall(common.internalCallback, () -> builder.forPath(path));
    }
}
