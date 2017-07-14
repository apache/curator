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
package org.apache.curator.x.async.modeled.migrations;

import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.modeled.ModelSerializer;
import org.apache.curator.x.async.modeled.ZPath;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;

public class MigrationManagerBuilder
{
    private final AsyncCuratorFramework client;
    private final ZPath lockPath;
    private final ModelSerializer<MetaData> metaDataSerializer;
    private final List<MigrationSet> sets = new ArrayList<>();
    private Executor executor = Runnable::run;
    private Duration lockMax = Duration.ofSeconds(15);

    MigrationManagerBuilder(AsyncCuratorFramework client, ZPath lockPath, ModelSerializer<MetaData> metaDataSerializer)
    {
        this.client = Objects.requireNonNull(client, "client cannot be null");
        this.lockPath = Objects.requireNonNull(lockPath, "lockPath cannot be null");
        this.metaDataSerializer = Objects.requireNonNull(metaDataSerializer, "metaDataSerializer cannot be null");
    }

    public MigrationManager build()
    {
        return new MigrationManagerImpl(client, lockPath, metaDataSerializer, executor, lockMax, sets);
    }

    public MigrationManagerBuilder withExecutor(Executor executor)
    {
        this.executor = Objects.requireNonNull(executor, "executor cannot be null");
        return this;
    }

    public MigrationManagerBuilder withLockMax(Duration lockMax)
    {
        this.lockMax = Objects.requireNonNull(lockMax, "lockMax cannot be null");
        return this;
    }

    public MigrationManagerBuilder adding(MigrationSet set)
    {
        sets.add(Objects.requireNonNull(set, "set cannot be null"));
        return this;
    }
}
