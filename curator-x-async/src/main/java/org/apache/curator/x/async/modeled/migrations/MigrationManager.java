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

import com.google.common.base.Throwables;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.modeled.ModelSerializer;
import org.apache.curator.x.async.modeled.ModelSpec;
import org.apache.curator.x.async.modeled.ModeledFramework;
import org.apache.curator.x.async.modeled.ZNode;
import org.apache.curator.x.async.modeled.ZPath;
import org.apache.zookeeper.CreateMode;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.curator.x.async.AsyncWrappers.*;

/**
 * Manages migrations
 */
public class MigrationManager
{
    private final AsyncCuratorFramework client;
    private final ZPath lockPath;
    private final ModelSerializer<MetaData> metaDataSerializer;
    private final Executor executor;
    private final Duration lockMax;

    private static final String META_DATA_NODE_NAME = "meta-";

    /**
     * Jackson usage: See the note in {@link org.apache.curator.x.async.modeled.JacksonModelSerializer} regarding how the Jackson library is specified in Curator's Maven file.
     * Unless you are not using Jackson pass <code>JacksonModelSerializer.build(MetaData.class)</code> for <code>metaDataSerializer</code>
     *
     * @param client the curator client
     * @param lockPath base path for locks used by the manager
     * @param metaDataSerializer JacksonModelSerializer.build(MetaData.class)
     * @param executor the executor to use
     * @param lockMax max time to wait for locks
     */
    public MigrationManager(AsyncCuratorFramework client, ZPath lockPath, ModelSerializer<MetaData> metaDataSerializer, Executor executor, Duration lockMax)
    {
        this.client = Objects.requireNonNull(client, "client cannot be null");
        this.lockPath = Objects.requireNonNull(lockPath, "lockPath cannot be null");
        this.metaDataSerializer = Objects.requireNonNull(metaDataSerializer, "metaDataSerializer cannot be null");
        this.executor = Objects.requireNonNull(executor, "executor cannot be null");
        this.lockMax = Objects.requireNonNull(lockMax, "lockMax cannot be null");
    }

    /**
     * Process the given migration set
     *
     * @param set the set
     * @return completion stage. If there is a migration-specific error, the stage will be completed
     * exceptionally with {@link org.apache.curator.x.async.modeled.migrations.MigrationException}.
     */
    public CompletionStage<Void> migrate(MigrationSet set)
    {
        String lockPath = this.lockPath.child(set.id()).fullPath();
        InterProcessLock lock = new InterProcessSemaphoreMutex(client.unwrap(), lockPath);
        CompletionStage<Void> lockStage = lockAsync(lock, lockMax.toMillis(), TimeUnit.MILLISECONDS, executor);
        return lockStage.thenCompose(__ -> runMigrationInLock(lock, set));
    }

    /**
     * Utility to return the meta data from previous migrations
     *
     * @param set the set
     * @return stage
     */
    public CompletionStage<List<MetaData>> metaData(MigrationSet set)
    {
        ModeledFramework<MetaData> modeled = getMetaDataClient(set.metaDataPath());
        return ZNode.models(modeled.childrenAsZNodes());
    }

    /**
     * Can be overridden to change how the comparison to previous migrations is done. The default
     * version ensures that the meta data from previous migrations matches the current migration
     * set exactly (by order and version). If there is a mismatch, <code>MigrationException</code> is thrown.
     *
     * @param set the migration set being applied
     * @param sortedMetaData previous migration meta data (may be empty)
     * @return the list of actual migrations to perform. The filter can return any value here or an empty list.
     * @throws MigrationException errors
     */
    protected List<Migration> filter(MigrationSet set, List<MetaData> sortedMetaData) throws MigrationException
    {
        if ( sortedMetaData.size() > set.migrations().size() )
        {
            throw new MigrationException(set.id(), String.format("More metadata than migrations. Migration ID: %s - MigrationSet: %s - MetaData: %s", set.id(), set.migrations(), sortedMetaData));
        }

        int compareSize = Math.min(set.migrations().size(), sortedMetaData.size());
        List<MetaData> compareMigrations = set.migrations().subList(0, compareSize)
            .stream()
            .map(m -> new MetaData(m.id(), m.version()))
            .collect(Collectors.toList());
        if ( !compareMigrations.equals(sortedMetaData) )
        {
            throw new MigrationException(set.id(), String.format("Metadata mismatch. Migration ID: %s - MigrationSet: %s - MetaData: %s", set.id(), set.migrations(), sortedMetaData));
        }
        return set.migrations().subList(sortedMetaData.size(), set.migrations().size());
    }

    private CompletionStage<Void> runMigrationInLock(InterProcessLock lock, MigrationSet set)
    {
        ModeledFramework<MetaData> modeled = getMetaDataClient(set.metaDataPath());
        return modeled.childrenAsZNodes()
            .thenCompose(metaData -> applyMetaData(set, modeled, metaData))
            .handle((v, e) -> {
                release(lock, true);
                if ( e != null )
                {
                    Throwables.propagate(e);
                }
                return v;
            }
        );
    }

    private ModeledFramework<MetaData> getMetaDataClient(ZPath metaDataPath)
    {
        ModelSpec<MetaData> modelSpec = ModelSpec.builder(metaDataPath, metaDataSerializer).withCreateMode(CreateMode.PERSISTENT_SEQUENTIAL).build();
        return ModeledFramework.wrap(client, modelSpec);
    }

    private CompletionStage<Void> applyMetaData(MigrationSet set, ModeledFramework<MetaData> metaDataClient, List<ZNode<MetaData>> metaDataNodes)
    {
        List<MetaData> sortedMetaData = metaDataNodes
            .stream()
            .sorted(Comparator.comparing(m -> m.path().fullPath()))
            .map(ZNode::model)
            .collect(Collectors.toList());

        List<Migration> toBeApplied;
        try
        {
            toBeApplied = filter(set, sortedMetaData);
        }
        catch ( MigrationException e )
        {
            CompletableFuture<Void> future = new CompletableFuture<>();
            future.completeExceptionally(e);
            return future;
        }

        if ( toBeApplied.size() == 0 )
        {
            return CompletableFuture.completedFuture(null);
        }

        return asyncEnsureContainers(client, metaDataClient.modelSpec().path())
            .thenCompose(__ -> applyMetaDataAfterEnsure(toBeApplied, metaDataClient));
    }

    private CompletionStage<Void> applyMetaDataAfterEnsure(List<Migration> toBeApplied, ModeledFramework<MetaData> metaDataClient)
    {
        List<CompletableFuture<Object>> stages = toBeApplied.stream().map(migration -> {
            List<CuratorOp> operations = new ArrayList<>();
            operations.addAll(migration.operations());
            MetaData thisMetaData = new MetaData(migration.id(), migration.version());
            operations.add(metaDataClient.child(META_DATA_NODE_NAME).createOp(thisMetaData));
            return client.transaction().forOperations(operations).thenApply(__ -> null).toCompletableFuture();
        }).collect(Collectors.toList());
        return CompletableFuture.allOf(stages.toArray(new CompletableFuture[stages.size()]));
    }
}
