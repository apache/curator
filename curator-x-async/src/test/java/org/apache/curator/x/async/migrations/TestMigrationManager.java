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
package org.apache.curator.x.async.migrations;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import com.google.common.base.Throwables;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.AsyncWrappers;
import org.apache.curator.x.async.CompletableBaseClassForTests;
import org.apache.curator.x.async.migrations.models.ModelV1;
import org.apache.curator.x.async.migrations.models.ModelV2;
import org.apache.curator.x.async.migrations.models.ModelV3;
import org.apache.curator.x.async.modeled.JacksonModelSerializer;
import org.apache.curator.x.async.modeled.ModelSpec;
import org.apache.curator.x.async.modeled.ModeledFramework;
import org.apache.curator.x.async.modeled.ZPath;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class TestMigrationManager extends CompletableBaseClassForTests
{
    private static final String LOCK_PATH = "/migrations/locks";
    private static final String META_DATA_PATH = "/migrations/metadata";
    private AsyncCuratorFramework client;
    private ModelSpec<ModelV1> v1Spec;
    private ModelSpec<ModelV2> v2Spec;
    private ModelSpec<ModelV3> v3Spec;
    private ExecutorService executor;
    private CuratorOp v1opA;
    private CuratorOp v1opB;
    private CuratorOp v2op;
    private CuratorOp v3op;
    private MigrationManager manager;
    private final AtomicReference<CountDownLatch> filterLatch = new AtomicReference<>();
    private CountDownLatch filterIsSetLatch;

    @BeforeEach
    @Override
    public void setup() throws Exception
    {
        super.setup();

        filterIsSetLatch = new CountDownLatch(1);

        CuratorFramework rawClient = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(100));
        rawClient.start();

        this.client = AsyncCuratorFramework.wrap(rawClient);

        ZPath modelPath = ZPath.parse("/test/it");

        v1Spec = ModelSpec.builder(modelPath, JacksonModelSerializer.build(ModelV1.class)).build();
        v2Spec = ModelSpec.builder(modelPath, JacksonModelSerializer.build(ModelV2.class)).build();
        v3Spec = ModelSpec.builder(modelPath, JacksonModelSerializer.build(ModelV3.class)).build();

        v1opA = client.unwrap().transactionOp().create().forPath(v1Spec.path().parent().fullPath());
        v1opB = ModeledFramework.wrap(client, v1Spec).createOp(new ModelV1("Test"));
        v2op = ModeledFramework.wrap(client, v2Spec).updateOp(new ModelV2("Test 2", 10));
        v3op = ModeledFramework.wrap(client, v3Spec).updateOp(new ModelV3("One", "Two", 30));

        executor = Executors.newCachedThreadPool();
        manager = new MigrationManager(client, LOCK_PATH, META_DATA_PATH, executor, Duration.ofMinutes(10))
        {
            @Override
            protected List<Migration> filter(MigrationSet set, List<byte[]> operationHashesInOrder) throws MigrationException
            {
                CountDownLatch localLatch = filterLatch.getAndSet(null);
                if ( localLatch != null )
                {
                    filterIsSetLatch.countDown();
                    try
                    {
                        localLatch.await();
                    }
                    catch ( InterruptedException e )
                    {
                        Thread.currentThread().interrupt();
                        Throwables.propagate(e);
                    }
                }
                return super.filter(set, operationHashesInOrder);
            }
        };
        manager.debugCount = new AtomicInteger();
    }

    @AfterEach
    @Override
    public void teardown() throws Exception
    {
        CloseableUtils.closeQuietly(client.unwrap());
        executor.shutdownNow();
        super.teardown();
    }

    @Test
    public void testBasic()
    {
        Migration m1 = () -> Arrays.asList(v1opA, v1opB);
        Migration m2 = () -> Collections.singletonList(v2op);
        Migration m3 = () -> Collections.singletonList(v3op);
        MigrationSet migrationSet = MigrationSet.build("1", Arrays.asList(m1, m2, m3));

        complete(manager.migrate(migrationSet));

        ModeledFramework<ModelV3> v3Client = ModeledFramework.wrap(client, v3Spec);
        complete(v3Client.read(), (m, e) -> {
            assertEquals(m.getAge(), 30);
            assertEquals(m.getFirstName(), "One");
            assertEquals(m.getLastName(), "Two");
        });

        int count = manager.debugCount.get();
        complete(manager.migrate(migrationSet));
        assertEquals(manager.debugCount.get(), count);   // second call should do nothing
    }

    @Test
    public void testStaged()
    {
        Migration m1 = () -> Arrays.asList(v1opA, v1opB);
        MigrationSet migrationSet = MigrationSet.build("1", Collections.singletonList(m1));
        complete(manager.migrate(migrationSet));

        ModeledFramework<ModelV1> v1Client = ModeledFramework.wrap(client, v1Spec);
        complete(v1Client.read(), (m, e) -> assertEquals(m.getName(), "Test"));

        Migration m2 = () -> Collections.singletonList(v2op);
        migrationSet = MigrationSet.build("1", Arrays.asList(m1, m2));
        complete(manager.migrate(migrationSet));

        ModeledFramework<ModelV2> v2Client = ModeledFramework.wrap(client, v2Spec);
        complete(v2Client.read(), (m, e) -> {
            assertEquals(m.getName(), "Test 2");
            assertEquals(m.getAge(), 10);
        });

        Migration m3 = () -> Collections.singletonList(v3op);
        migrationSet = MigrationSet.build("1", Arrays.asList(m1, m2, m3));
        complete(manager.migrate(migrationSet));

        ModeledFramework<ModelV3> v3Client = ModeledFramework.wrap(client, v3Spec);
        complete(v3Client.read(), (m, e) -> {
            assertEquals(m.getAge(), 30);
            assertEquals(m.getFirstName(), "One");
            assertEquals(m.getLastName(), "Two");
        });
    }

    @Test
    public void testDocExample() throws Exception
    {
        CuratorOp op1 = client.transactionOp().create().forPath("/parent");
        CuratorOp op2 = client.transactionOp().create().forPath("/parent/one");
        CuratorOp op3 = client.transactionOp().create().forPath("/parent/two");
        CuratorOp op4 = client.transactionOp().create().forPath("/parent/three");
        CuratorOp op5 = client.transactionOp().create().forPath("/main", "hey".getBytes());

        Migration initialMigration = () -> Arrays.asList(op1, op2, op3, op4, op5);
        MigrationSet migrationSet = MigrationSet.build("main", Collections.singletonList(initialMigration));
        complete(manager.migrate(migrationSet));

        assertNotNull(client.unwrap().checkExists().forPath("/parent/three"));
        assertArrayEquals(client.unwrap().getData().forPath("/main"), "hey".getBytes());

        CuratorOp newOp1 = client.transactionOp().create().forPath("/new");
        CuratorOp newOp2 = client.transactionOp().delete().forPath("/main");    // maybe this is no longer needed

        Migration newMigration = () -> Arrays.asList(newOp1, newOp2);
        migrationSet = MigrationSet.build("main", Arrays.asList(initialMigration, newMigration));
        complete(manager.migrate(migrationSet));

        assertNull(client.unwrap().checkExists().forPath("/main"));
    }

    @Test
    public void testChecksumDataError()
    {
        CuratorOp op1 = client.transactionOp().create().forPath("/test");
        CuratorOp op2 = client.transactionOp().create().forPath("/test/bar", "first".getBytes());
        Migration migration = () -> Arrays.asList(op1, op2);
        MigrationSet migrationSet = MigrationSet.build("1", Collections.singletonList(migration));
        complete(manager.migrate(migrationSet));

        CuratorOp op2Changed = client.transactionOp().create().forPath("/test/bar", "second".getBytes());
        migration = () -> Arrays.asList(op1, op2Changed);
        migrationSet = MigrationSet.build("1", Collections.singletonList(migration));
        try
        {
            complete(manager.migrate(migrationSet));
            fail("Should throw");
        }
        catch ( Throwable e )
        {
            assertTrue(Throwables.getRootCause(e) instanceof MigrationException);
        }
    }

    @Test
    public void testChecksumPathError()
    {
        CuratorOp op1 = client.transactionOp().create().forPath("/test2");
        CuratorOp op2 = client.transactionOp().create().forPath("/test2/bar");
        Migration migration = () -> Arrays.asList(op1, op2);
        MigrationSet migrationSet = MigrationSet.build("1", Collections.singletonList(migration));
        complete(manager.migrate(migrationSet));

        CuratorOp op2Changed = client.transactionOp().create().forPath("/test/bar");
        migration = () -> Arrays.asList(op1, op2Changed);
        migrationSet = MigrationSet.build("1", Collections.singletonList(migration));
        try
        {
            complete(manager.migrate(migrationSet));
            fail("Should throw");
        }
        catch ( Throwable e )
        {
            assertTrue(Throwables.getRootCause(e) instanceof MigrationException);
        }
    }

    @Test
    public void testPartialApplyForBadOps() throws Exception
    {
        CuratorOp op1 = client.transactionOp().create().forPath("/test", "something".getBytes());
        CuratorOp op2 = client.transactionOp().create().forPath("/a/b/c");
        Migration m1 = () -> Collections.singletonList(op1);
        Migration m2 = () -> Collections.singletonList(op2);
        MigrationSet migrationSet = MigrationSet.build("1", Arrays.asList(m1, m2));
        try
        {
            complete(manager.migrate(migrationSet));
            fail("Should throw");
        }
        catch ( Throwable e )
        {
            assertTrue(Throwables.getRootCause(e) instanceof KeeperException.NoNodeException);
        }

        assertNull(client.unwrap().checkExists().forPath("/test"));  // should be all or nothing
    }

    @Test
    public void testTransactionForBadOps() throws Exception
    {
        CuratorOp op1 = client.transactionOp().create().forPath("/test2", "something".getBytes());
        CuratorOp op2 = client.transactionOp().create().forPath("/a/b/c/d");
        Migration migration = () -> Arrays.asList(op1, op2);
        MigrationSet migrationSet = MigrationSet.build("1", Collections.singletonList(migration));
        try
        {
            complete(manager.migrate(migrationSet));
            fail("Should throw");
        }
        catch ( Throwable e )
        {
            assertTrue(Throwables.getRootCause(e) instanceof KeeperException.NoNodeException);
        }

        assertNull(client.unwrap().checkExists().forPath("/test"));
    }

    @Test
    public void testConcurrency1() throws Exception
    {
        CuratorOp op1 = client.transactionOp().create().forPath("/test");
        CuratorOp op2 = client.transactionOp().create().forPath("/test/bar", "first".getBytes());
        Migration migration = () -> Arrays.asList(op1, op2);
        MigrationSet migrationSet = MigrationSet.build("1", Collections.singletonList(migration));
        CountDownLatch latch = new CountDownLatch(1);
        filterLatch.set(latch);
        CompletionStage<Void> first = manager.migrate(migrationSet);
        assertTrue(timing.awaitLatch(filterIsSetLatch));

        MigrationManager manager2 = new MigrationManager(client, LOCK_PATH, META_DATA_PATH, executor, Duration.ofMillis(timing.forSleepingABit().milliseconds()));
        try
        {
            complete(manager2.migrate(migrationSet));
            fail("Should throw");
        }
        catch ( Throwable e )
        {
            assertTrue(Throwables.getRootCause(e) instanceof AsyncWrappers.TimeoutException, "Should throw AsyncWrappers.TimeoutException, was: " + Throwables.getStackTraceAsString(Throwables.getRootCause(e)));
        }

        latch.countDown();
        complete(first);
        assertArrayEquals(client.unwrap().getData().forPath("/test/bar"), "first".getBytes());
    }

    @Test
    public void testConcurrency2() throws Exception
    {
        CuratorOp op1 = client.transactionOp().create().forPath("/test");
        CuratorOp op2 = client.transactionOp().create().forPath("/test/bar", "first".getBytes());
        Migration migration = () -> Arrays.asList(op1, op2);
        MigrationSet migrationSet = MigrationSet.build("1", Collections.singletonList(migration));
        CountDownLatch latch = new CountDownLatch(1);
        filterLatch.set(latch);
        CompletionStage<Void> first = manager.migrate(migrationSet);
        assertTrue(timing.awaitLatch(filterIsSetLatch));

        CompletionStage<Void> second = manager.migrate(migrationSet);
        try
        {
            second.toCompletableFuture().get(timing.forSleepingABit().milliseconds(), TimeUnit.MILLISECONDS);
            fail("Should throw");
        }
        catch ( Throwable e )
        {
            assertTrue(Throwables.getRootCause(e) instanceof TimeoutException, "Should throw TimeoutException, was: " + Throwables.getStackTraceAsString(Throwables.getRootCause(e)));
        }

        latch.countDown();
        complete(first);
        assertArrayEquals(client.unwrap().getData().forPath("/test/bar"), "first".getBytes());
        complete(second);
        assertEquals(manager.debugCount.get(), 1);
    }
}
