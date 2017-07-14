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
import com.google.common.collect.ImmutableList;
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
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.curator.x.async.AsyncWrappers.*;

class MigrationManagerImpl implements MigrationManager
{
    private final AsyncCuratorFramework client;
    private final ZPath lockPath;
    private final ModelSerializer<MetaData> metaDataSerializer;
    private final Executor executor;
    private final Duration lockMax;
    private final List<MigrationSet> sets;

    private static final String META_DATA_NODE_NAME = "meta-";

    MigrationManagerImpl(AsyncCuratorFramework client, ZPath lockPath, ModelSerializer<MetaData> metaDataSerializer, Executor executor, Duration lockMax, List<MigrationSet> sets)
    {
        this.client = client;
        this.lockPath = lockPath;
        this.metaDataSerializer = metaDataSerializer;
        this.executor = executor;
        this.lockMax = lockMax;
        this.sets = ImmutableList.copyOf(sets);
    }

    @Override
    public CompletionStage<List<MetaData>> metaData(ZPath metaDataPath)
    {
        ModeledFramework<MetaData> modeled = getMetaDataClient(metaDataPath);
        return ZNode.models(modeled.childrenAsZNodes());
    }

    @Override
    public CompletionStage<Void> run()
    {
        Map<String, CompletableFuture<Void>> futures = sets
            .stream()
            .map(m -> new AbstractMap.SimpleEntry<>(m.id(), runMigration(m).toCompletableFuture()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        return CompletableFuture.allOf(futures.values().toArray(new CompletableFuture[futures.size()]));
    }

    private CompletionStage<Void> runMigration(MigrationSet set)
    {
        String lockPath = this.lockPath.child(set.id()).fullPath();
        InterProcessLock lock = new InterProcessSemaphoreMutex(client.unwrap(), lockPath);
        CompletionStage<Void> lockStage = lockAsync(lock, lockMax.toMillis(), TimeUnit.MILLISECONDS, executor);
        return lockStage.thenCompose(__ -> runMigrationInLock(lock, set));
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

    protected void checkIsValid(MigrationSet set, List<MetaData> sortedMetaData) throws InvalidMigrationSetException
    {
        if ( sortedMetaData.size() > set.migrations().size() )
        {
            throw new InvalidMigrationSetException(set.id(), String.format("More metadata than migrations. Migration ID: %s - MetaData: %s", set.id(), sortedMetaData));
        }

        int compareSize = Math.min(set.migrations().size(), sortedMetaData.size());
        List<MetaData> compareMigrations = set.migrations().subList(0, compareSize)
            .stream()
            .map(m -> new MetaData(m.id(), m.version()))
            .collect(Collectors.toList());
        if ( !compareMigrations.equals(sortedMetaData) )
        {
            throw new InvalidMigrationSetException(set.id(), String.format("Metadata mismatch. Migration ID: %s - MetaData: %s", set.id(), sortedMetaData));
        }
    }

    private CompletionStage<Void> applyMetaData(MigrationSet set, ModeledFramework<MetaData> metaDataClient, List<ZNode<MetaData>> metaDataNodes)
    {
        List<MetaData> sortedMetaData = metaDataNodes
            .stream()
            .sorted(Comparator.comparing(m -> m.path().fullPath()))
            .map(ZNode::model)
            .collect(Collectors.toList());
        try
        {
            checkIsValid(set, sortedMetaData);
        }
        catch ( InvalidMigrationSetException e )
        {
            CompletableFuture<Void> future = new CompletableFuture<>();
            future.completeExceptionally(e);
            return future;
        }

        List<Migration> toBeApplied = set.migrations().subList(sortedMetaData.size(), set.migrations().size());
        if ( toBeApplied.size() == 0 )
        {
            return CompletableFuture.completedFuture(null);
        }

        return asyncEnsureContainers(client, metaDataClient.modelSpec().path())
            .thenCompose(__ -> applyMetaDataAfterEnsure(set, toBeApplied, metaDataClient));
    }

    private CompletionStage<Void> applyMetaDataAfterEnsure(MigrationSet set, List<Migration> toBeApplied, ModeledFramework<MetaData> metaDataClient)
    {
        ModelSpec<byte[]> modelSpec = ModelSpec.builder(set.path(), ModelSerializer.raw).build();
        ModeledFramework<byte[]> modeled = ModeledFramework.wrap(client, modelSpec);
        return modeled.childrenAsZNodes().thenCompose(nodes -> {
            List<CuratorOp> operations = new ArrayList<>();
            for ( ZNode<byte[]> node : nodes )
            {
                byte[] currentBytes = node.model();
                for ( Migration migration : toBeApplied )
                {
                    currentBytes = migration.migrate(currentBytes);
                    MetaData thisMetaData = new MetaData(migration.id(), migration.version());
                    operations.add(metaDataClient.child(META_DATA_NODE_NAME).createOp(thisMetaData));
                }
                operations.add(modeled.child(node.path().nodeName()).updateOp(currentBytes, node.stat().getVersion()));
            }
            return client.transaction().forOperations(operations).thenApply(__ -> null);
        });
    }
}
