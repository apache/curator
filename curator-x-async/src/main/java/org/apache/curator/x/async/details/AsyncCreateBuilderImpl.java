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

import org.apache.curator.framework.imps.CreateBuilderImpl;
import org.apache.curator.framework.imps.CuratorFrameworkImpl;
import org.apache.curator.x.async.AsyncStage;
import org.apache.curator.x.async.api.AsyncCreateBuilder;
import org.apache.curator.x.async.api.AsyncPathAndBytesable;
import org.apache.curator.x.async.api.CreateOption;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.apache.curator.x.async.details.BackgroundProcs.nameProc;
import static org.apache.curator.x.async.details.BackgroundProcs.safeCall;

class AsyncCreateBuilderImpl implements AsyncCreateBuilder
{
    private final CuratorFrameworkImpl client;
    private final Filters filters;
    private CreateMode createMode = CreateMode.PERSISTENT;
    private List<ACL> aclList = null;
    private Set<CreateOption> options = Collections.emptySet();
    private Stat stat = null;
    private long ttl = -1;
    private int setDataVersion = -1;

    AsyncCreateBuilderImpl(CuratorFrameworkImpl client, Filters filters)
    {
        this.client = client;
        this.filters = filters;
    }

    @Override
    public AsyncPathAndBytesable<AsyncStage<String>> storingStatIn(Stat stat)
    {
        this.stat = stat;
        return this;
    }

    @Override
    public AsyncPathAndBytesable<AsyncStage<String>> withMode(CreateMode createMode)
    {
        this.createMode = Objects.requireNonNull(createMode, "createMode cannot be null");
        return this;
    }

    @Override
    public AsyncPathAndBytesable<AsyncStage<String>> withACL(List<ACL> aclList)
    {
        this.aclList = aclList;
        return this;
    }

    @Override
    public AsyncPathAndBytesable<AsyncStage<String>> withTtl(long ttl)
    {
        this.ttl = ttl;
        return this;
    }

    @Override
    public AsyncPathAndBytesable<AsyncStage<String>> withSetDataVersion(int version)
    {
        this.setDataVersion = version;
        return this;
    }

    @Override
    public AsyncPathAndBytesable<AsyncStage<String>> withOptions(Set<CreateOption> options)
    {
        this.options = Objects.requireNonNull(options, "options cannot be null");
        return this;
    }

    @Override
    public AsyncPathAndBytesable<AsyncStage<String>> withOptions(Set<CreateOption> options, List<ACL> aclList)
    {
        this.options = Objects.requireNonNull(options, "options cannot be null");
        this.aclList = aclList;
        return this;
    }

    @Override
    public AsyncPathAndBytesable<AsyncStage<String>> withOptions(Set<CreateOption> options, CreateMode createMode, List<ACL> aclList)
    {
        this.options = Objects.requireNonNull(options, "options cannot be null");
        this.aclList = aclList;
        this.createMode = Objects.requireNonNull(createMode, "createMode cannot be null");
        return this;
    }

    @Override
    public AsyncPathAndBytesable<AsyncStage<String>> withOptions(Set<CreateOption> options, CreateMode createMode)
    {
        this.options = Objects.requireNonNull(options, "options cannot be null");
        this.createMode = Objects.requireNonNull(createMode, "createMode cannot be null");
        return this;
    }

    @Override
    public AsyncPathAndBytesable<AsyncStage<String>> withOptions(Set<CreateOption> options, CreateMode createMode, List<ACL> aclList, Stat stat)
    {
        this.options = Objects.requireNonNull(options, "options cannot be null");
        this.aclList = aclList;
        this.createMode = Objects.requireNonNull(createMode, "createMode cannot be null");
        this.stat = stat;
        return this;
    }

    @Override
    public AsyncPathAndBytesable<AsyncStage<String>> withOptions(Set<CreateOption> options, CreateMode createMode, List<ACL> aclList, Stat stat, long ttl)
    {
        this.options = Objects.requireNonNull(options, "options cannot be null");
        this.aclList = aclList;
        this.createMode = Objects.requireNonNull(createMode, "createMode cannot be null");
        this.stat = stat;
        this.ttl = ttl;
        return this;
    }

    @Override
    public AsyncPathAndBytesable<AsyncStage<String>> withOptions(Set<CreateOption> options, CreateMode createMode, List<ACL> aclList, Stat stat, long ttl, int setDataVersion)
    {
        this.options = Objects.requireNonNull(options, "options cannot be null");
        this.aclList = aclList;
        this.createMode = Objects.requireNonNull(createMode, "createMode cannot be null");
        this.stat = stat;
        this.ttl = ttl;
        this.setDataVersion = setDataVersion;
        return this;
    }

    @Override
    public AsyncStage<String> forPath(String path)
    {
        return internalForPath(path, null, false);
    }

    @Override
    public AsyncStage<String> forPath(String path, byte[] data)
    {
        return internalForPath(path, data, true);
    }

    private AsyncStage<String> internalForPath(String path, byte[] data, boolean useData)
    {
        BuilderCommon<String> common = new BuilderCommon<>(filters, nameProc);
        CreateBuilderImpl builder = new CreateBuilderImpl(client,
            createMode,
            common.backgrounding,
            options.contains(CreateOption.createParentsIfNeeded) || options.contains(CreateOption.createParentsAsContainers),
            options.contains(CreateOption.createParentsAsContainers),
            options.contains(CreateOption.doProtected),
            options.contains(CreateOption.compress),
            options.contains(CreateOption.setDataIfExists),
            aclList,
            stat,
            ttl
        );
        builder.setSetDataIfExistsVersion(setDataVersion);
        return safeCall(common.internalCallback, () -> useData ? builder.forPath(path, data) : builder.forPath(path));
    }
}
