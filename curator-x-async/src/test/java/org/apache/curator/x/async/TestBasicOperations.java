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

package org.apache.curator.x.async;

import static java.util.EnumSet.of;
import static org.apache.curator.x.async.api.CreateOption.compress;
import static org.apache.curator.x.async.api.CreateOption.setDataIfExists;
import static org.apache.zookeeper.CreateMode.EPHEMERAL_SEQUENTIAL;
import static org.apache.zookeeper.CreateMode.PERSISTENT_SEQUENTIAL;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestBasicOperations extends CompletableBaseClassForTests {
    private AsyncCuratorFramework client;

    @BeforeEach
    @Override
    public void setup() throws Exception {
        super.setup();

        CuratorFramework client = CuratorFrameworkFactory.newClient(
                server.getConnectString(),
                new RetryOneTime(timing.forSleepingABit().milliseconds()));
        client.start();
        this.client = AsyncCuratorFramework.wrap(client);
    }

    @AfterEach
    @Override
    public void teardown() throws Exception {
        CloseableUtils.closeQuietly(client.unwrap());

        super.teardown();
    }

    @Test
    public void testCreateTransactionWithMode() throws Exception {
        complete(AsyncWrappers.asyncEnsureContainers(client, "/test"));

        CuratorOp op1 =
                client.transactionOp().create().withMode(PERSISTENT_SEQUENTIAL).forPath("/test/node-");
        CuratorOp op2 =
                client.transactionOp().create().withMode(PERSISTENT_SEQUENTIAL).forPath("/test/node-");
        complete(client.transaction().forOperations(Arrays.asList(op1, op2)));

        assertEquals(client.unwrap().getChildren().forPath("/test").size(), 2);
    }

    @Test
    public void testCrud() {
        AsyncStage<String> createStage = client.create().forPath("/test", "one".getBytes());
        complete(createStage, (path, e) -> assertEquals(path, "/test"));

        AsyncStage<byte[]> getStage = client.getData().forPath("/test");
        complete(getStage, (data, e) -> assertArrayEquals(data, "one".getBytes()));

        CompletionStage<byte[]> combinedStage = client.setData()
                .forPath("/test", "new".getBytes())
                .thenCompose(__ -> client.getData().forPath("/test"));
        complete(combinedStage, (data, e) -> assertArrayEquals(data, "new".getBytes()));

        CompletionStage<Void> combinedDelete = client.create()
                .withMode(EPHEMERAL_SEQUENTIAL)
                .forPath("/deleteme")
                .thenCompose(path -> client.delete().forPath(path));
        complete(combinedDelete, (v, e) -> assertNull(e));

        CompletionStage<byte[]> setDataIfStage = client.create()
                .withOptions(of(compress, setDataIfExists))
                .forPath("/test", "last".getBytes())
                .thenCompose(__ -> client.getData().decompressed().forPath("/test"));
        complete(setDataIfStage, (data, e) -> assertArrayEquals(data, "last".getBytes()));
    }

    @Test
    public void testException() {
        CountDownLatch latch = new CountDownLatch(1);
        client.getData().forPath("/woop").exceptionally(e -> {
            assertTrue(e instanceof KeeperException);
            assertEquals(((KeeperException) e).code(), KeeperException.Code.NONODE);
            latch.countDown();
            return null;
        });
        assertTrue(timing.awaitLatch(latch));
    }

    @Test
    public void testWatching() {
        CountDownLatch latch = new CountDownLatch(1);
        client.watched().checkExists().forPath("/test").event().whenComplete((event, exception) -> {
            assertNull(exception);
            assertEquals(event.getType(), Watcher.Event.EventType.NodeCreated);
            latch.countDown();
        });
        client.create().forPath("/test");
        assertTrue(timing.awaitLatch(latch));
    }

    @Test
    public void testWatchingWithServerLoss() throws Exception {
        AsyncStage<Stat> stage = client.watched().checkExists().forPath("/test");
        stage.thenRun(() -> {
            try {
                server.stop();
            } catch (IOException e) {
                // ignore
            }
        });

        CountDownLatch latch = new CountDownLatch(1);
        complete(stage.event(), (v, e) -> {
            assertTrue(e instanceof AsyncEventException);
            assertEquals(((AsyncEventException) e).getKeeperState(), Watcher.Event.KeeperState.Disconnected);
            ((AsyncEventException) e).reset().thenRun(latch::countDown);
        });

        server.restart();
        client.create().forPath("/test");
        assertTrue(timing.awaitLatch(latch));
    }

    @Test
    public void testResultWrapper() throws Exception {
        CompletionStage<AsyncResult<String>> resultStage =
                AsyncResult.of(client.create().forPath("/first"));
        complete(resultStage, (v, e) -> {
            assertNull(e);
            assertEquals(v.getRawValue(), "/first");
            assertNull(v.getRawException());
            assertEquals(v.getCode(), KeeperException.Code.OK);
        });

        resultStage = AsyncResult.of(client.create().forPath("/foo/bar"));
        complete(resultStage, (v, e) -> {
            assertNull(e);
            assertNull(v.getRawValue());
            assertNull(v.getRawException());
            assertEquals(v.getCode(), KeeperException.Code.NONODE);
        });

        resultStage = AsyncResult.of(client.create().forPath("illegal path"));
        complete(resultStage, (v, e) -> {
            assertNull(e);
            assertNull(v.getRawValue());
            assertNotNull(v.getRawException());
            assertTrue(v.getRawException() instanceof IllegalArgumentException);
            assertEquals(v.getCode(), KeeperException.Code.SYSTEMERROR);
        });

        server.stop();
        resultStage = AsyncResult.of(client.create().forPath("/second"));
        complete(resultStage, (v, e) -> {
            assertNull(e);
            assertNull(v.getRawValue());
            assertNull(v.getRawException());
            assertEquals(v.getCode(), KeeperException.Code.CONNECTIONLOSS);
        });
    }

    @Test
    public void testGetDataWithStat() {
        complete(client.create().forPath("/test", "hey".getBytes()));

        Stat stat = new Stat();
        complete(client.getData().storingStatIn(stat).forPath("/test"));
        assertEquals(stat.getDataLength(), "hey".length());
    }
}
