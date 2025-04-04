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

import static org.apache.curator.x.async.details.BackgroundProcs.safeCall;
import static org.apache.curator.x.async.details.BackgroundProcs.statProc;
import java.util.List;
import org.apache.curator.framework.imps.CuratorFrameworkBase;
import org.apache.curator.framework.imps.SetACLBuilderImpl;
import org.apache.curator.x.async.AsyncStage;
import org.apache.curator.x.async.api.AsyncPathable;
import org.apache.curator.x.async.api.AsyncSetACLBuilder;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

class AsyncSetACLBuilderImpl implements AsyncSetACLBuilder, AsyncPathable<AsyncStage<Stat>> {
    private final CuratorFrameworkBase client;
    private final Filters filters;
    private int version = -1;
    private List<ACL> aclList = null;

    AsyncSetACLBuilderImpl(CuratorFrameworkBase client, Filters filters) {
        this.client = client;
        this.filters = filters;
    }

    @Override
    public AsyncPathable<AsyncStage<Stat>> withACL(List<ACL> aclList) {
        this.aclList = aclList;
        return this;
    }

    @Override
    public AsyncPathable<AsyncStage<Stat>> withACL(List<ACL> aclList, int version) {
        this.aclList = aclList;
        this.version = version;
        return this;
    }

    @Override
    public AsyncStage<Stat> forPath(String path) {
        BuilderCommon<Stat> common = new BuilderCommon<>(filters, statProc);
        SetACLBuilderImpl builder = new SetACLBuilderImpl(client, common.backgrounding, aclList, version);
        return safeCall(common.internalCallback, () -> builder.forPath(path));
    }
}
