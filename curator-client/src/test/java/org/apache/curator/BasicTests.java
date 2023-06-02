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

package org.apache.curator;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.curator.ensemble.fixed.FixedEnsembleProvider;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.ZookeeperFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class BasicTests extends BaseClassForTests {
    @Test
    public void testFactory() throws Exception {
        final ZooKeeper mockZookeeper = Mockito.mock(ZooKeeper.class);
        ZookeeperFactory zookeeperFactory = (connectString, sessionTimeout, watcher, canBeReadOnly) -> mockZookeeper;
        CuratorZookeeperClient client = new CuratorZookeeperClient(
                zookeeperFactory,
                new FixedEnsembleProvider(server.getConnectString()),
                10000,
                10000,
                null,
                new RetryOneTime(1),
                false);
        client.start();
        assertEquals(client.getZooKeeper(), mockZookeeper);
    }

    @Test
    public void testExpiredSession() throws Exception {
        // see http://wiki.apache.org/hadoop/ZooKeeper/FAQ#A4

        final Timing timing = new Timing();

        final CountDownLatch latch = new CountDownLatch(1);
        Watcher watcher = event -> {
            if (event.getState() == Watcher.Event.KeeperState.Expired) {
                latch.countDown();
            }
        };

        final CuratorZookeeperClient client = new CuratorZookeeperClient(
                server.getConnectString(), timing.session(), timing.connection(), watcher, new RetryOneTime(2));
        client.start();
        try {
            final AtomicBoolean firstTime = new AtomicBoolean(true);
            RetryLoop.callWithRetry(client, () -> {
                if (firstTime.compareAndSet(true, false)) {
                    try {
                        client.getZooKeeper()
                                .create("/foo", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    } catch (KeeperException.NodeExistsException ignore) {
                        // ignore
                    }

                    client.getZooKeeper().getTestable().injectSessionExpiration();

                    assertTrue(timing.awaitLatch(latch));
                }
                ZooKeeper zooKeeper = client.getZooKeeper();
                client.blockUntilConnectedOrTimedOut();
                assertNotNull(zooKeeper.exists("/foo", false));
                return null;
            });
        } finally {
            client.close();
        }
    }

    @Test
    public void testReconnect() throws Exception {
        CuratorZookeeperClient client =
                new CuratorZookeeperClient(server.getConnectString(), 10000, 10000, null, new RetryOneTime(1));
        client.start();
        try {
            client.blockUntilConnectedOrTimedOut();

            byte[] writtenData = {1, 2, 3};
            client.getZooKeeper().create("/test", writtenData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            Thread.sleep(1000);
            server.stop();
            Thread.sleep(1000);

            server.restart();
            assertTrue(client.blockUntilConnectedOrTimedOut());
            byte[] readData = client.getZooKeeper().getData("/test", false, null);
            assertArrayEquals(readData, writtenData);
        } finally {
            client.close();
        }
    }

    @Test
    public void testSimple() throws Exception {
        CuratorZookeeperClient client =
                new CuratorZookeeperClient(server.getConnectString(), 10000, 10000, null, new RetryOneTime(1));
        client.start();
        try {
            client.blockUntilConnectedOrTimedOut();
            String path = client.getZooKeeper()
                    .create("/test", new byte[] {1, 2, 3}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            assertEquals(path, "/test");
        } finally {
            client.close();
        }
    }

    @Test
    public void testBackgroundConnect() throws Exception {
        final int CONNECTION_TIMEOUT_MS = 4000;
        try (CuratorZookeeperClient client = new CuratorZookeeperClient(
                server.getConnectString(), 10000, CONNECTION_TIMEOUT_MS, null, new RetryOneTime(1))) {
            assertFalse(client.isConnected());
            client.start();
            Awaitility.await()
                    .atMost(Duration.ofMillis(CONNECTION_TIMEOUT_MS))
                    .untilAsserted(() -> Assertions.assertTrue(client.isConnected()));
        }
    }
}
