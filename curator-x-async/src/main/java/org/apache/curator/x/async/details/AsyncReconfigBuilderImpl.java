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

import static org.apache.curator.x.async.details.BackgroundProcs.ignoredProc;
import static org.apache.curator.x.async.details.BackgroundProcs.safeCall;
import java.util.List;
import org.apache.curator.framework.imps.CuratorFrameworkImpl;
import org.apache.curator.framework.imps.ReconfigBuilderImpl;
import org.apache.curator.x.async.AsyncStage;
import org.apache.curator.x.async.api.AsyncEnsemblable;
import org.apache.curator.x.async.api.AsyncReconfigBuilder;
import org.apache.zookeeper.data.Stat;

class AsyncReconfigBuilderImpl implements AsyncReconfigBuilder, AsyncEnsemblable<AsyncStage<Void>> {
    private final CuratorFrameworkImpl client;
    private final Filters filters;
    private Stat stat = null;
    private long fromConfig = -1;
    private List<String> newMembers = null;
    private List<String> joining = null;
    private List<String> leaving = null;

    AsyncReconfigBuilderImpl(CuratorFrameworkImpl client, Filters filters) {
        this.client = client;
        this.filters = filters;
    }

    @Override
    public AsyncEnsemblable<AsyncStage<Void>> withNewMembers(List<String> servers) {
        this.newMembers = servers;
        return this;
    }

    @Override
    public AsyncEnsemblable<AsyncStage<Void>> withJoiningAndLeaving(List<String> joining, List<String> leaving) {
        this.joining = joining;
        this.leaving = leaving;
        return this;
    }

    @Override
    public AsyncEnsemblable<AsyncStage<Void>> withNewMembers(List<String> servers, Stat stat) {
        this.newMembers = servers;
        this.stat = stat;
        return this;
    }

    @Override
    public AsyncEnsemblable<AsyncStage<Void>> withJoiningAndLeaving(
            List<String> joining, List<String> leaving, Stat stat) {
        this.joining = joining;
        this.leaving = leaving;
        return this;
    }

    @Override
    public AsyncEnsemblable<AsyncStage<Void>> withNewMembers(List<String> servers, Stat stat, long fromConfig) {
        this.newMembers = servers;
        this.stat = stat;
        this.fromConfig = fromConfig;
        return this;
    }

    @Override
    public AsyncEnsemblable<AsyncStage<Void>> withJoiningAndLeaving(
            List<String> joining, List<String> leaving, Stat stat, long fromConfig) {
        this.joining = joining;
        this.leaving = leaving;
        this.stat = stat;
        this.fromConfig = fromConfig;
        return this;
    }

    @Override
    public AsyncEnsemblable<AsyncStage<Void>> withNewMembers(List<String> servers, long fromConfig) {
        this.newMembers = servers;
        this.fromConfig = fromConfig;
        return this;
    }

    @Override
    public AsyncEnsemblable<AsyncStage<Void>> withJoiningAndLeaving(
            List<String> joining, List<String> leaving, long fromConfig) {
        this.joining = joining;
        this.leaving = leaving;
        this.fromConfig = fromConfig;
        return this;
    }

    @Override
    public AsyncStage<Void> forEnsemble() {
        BuilderCommon<Void> common = new BuilderCommon<>(filters, ignoredProc);
        ReconfigBuilderImpl builder =
                new ReconfigBuilderImpl(client, common.backgrounding, stat, fromConfig, newMembers, joining, leaving);
        return safeCall(common.internalCallback, () -> {
            builder.forEnsemble();
            return null;
        });
    }
}
