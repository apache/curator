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

import static org.apache.curator.x.async.details.BackgroundProcs.childrenProc;
import static org.apache.curator.x.async.details.BackgroundProcs.safeCall;
import java.util.List;
import org.apache.curator.framework.imps.CuratorFrameworkBase;
import org.apache.curator.framework.imps.GetChildrenBuilderImpl;
import org.apache.curator.x.async.AsyncStage;
import org.apache.curator.x.async.WatchMode;
import org.apache.curator.x.async.api.AsyncGetChildrenBuilder;
import org.apache.curator.x.async.api.AsyncPathable;
import org.apache.zookeeper.data.Stat;

class AsyncGetChildrenBuilderImpl implements AsyncGetChildrenBuilder {
    private final CuratorFrameworkBase client;
    private final Filters filters;
    private final WatchMode watchMode;
    private Stat stat = null;

    AsyncGetChildrenBuilderImpl(CuratorFrameworkBase client, Filters filters, WatchMode watchMode) {
        this.client = client;
        this.filters = filters;
        this.watchMode = watchMode;
    }

    @Override
    public AsyncStage<List<String>> forPath(String path) {
        BuilderCommon<List<String>> common = new BuilderCommon<>(filters, watchMode, childrenProc);
        GetChildrenBuilderImpl builder = new GetChildrenBuilderImpl(client, common.watcher, common.backgrounding, stat);
        return safeCall(common.internalCallback, () -> builder.forPath(path));
    }

    @Override
    public AsyncPathable<AsyncStage<List<String>>> storingStatIn(Stat stat) {
        this.stat = stat;
        return this;
    }
}
