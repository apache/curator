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
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import org.apache.curator.framework.imps.DeleteBuilderImpl;
import org.apache.curator.framework.imps.InternalCuratorFramework;
import org.apache.curator.x.async.AsyncStage;
import org.apache.curator.x.async.api.AsyncDeleteBuilder;
import org.apache.curator.x.async.api.AsyncPathable;
import org.apache.curator.x.async.api.DeleteOption;

class AsyncDeleteBuilderImpl implements AsyncDeleteBuilder {
    private final InternalCuratorFramework client;
    private final Filters filters;
    private Set<DeleteOption> options = Collections.emptySet();
    private int version = -1;

    AsyncDeleteBuilderImpl(InternalCuratorFramework client, Filters filters) {
        this.client = client;
        this.filters = filters;
    }

    @Override
    public AsyncPathable<AsyncStage<Void>> withOptions(Set<DeleteOption> options) {
        return withOptionsAndVersion(options, -1);
    }

    @Override
    public AsyncPathable<AsyncStage<Void>> withOptionsAndVersion(Set<DeleteOption> options, int version) {
        this.options = Objects.requireNonNull(options, "options cannot be null");
        this.version = version;
        return this;
    }

    @Override
    public AsyncPathable<AsyncStage<Void>> withVersion(int version) {
        this.version = version;
        return this;
    }

    @Override
    public AsyncStage<Void> forPath(String path) {
        BuilderCommon<Void> common = new BuilderCommon<>(filters, ignoredProc);
        DeleteBuilderImpl builder = new DeleteBuilderImpl(
                client,
                version,
                common.backgrounding,
                options.contains(DeleteOption.deletingChildrenIfNeeded),
                options.contains(DeleteOption.guaranteed),
                options.contains(DeleteOption.quietly));
        return safeCall(common.internalCallback, () -> builder.forPath(path));
    }
}
