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

package org.apache.curator.framework.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.google.common.collect.Queues;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.compatibility.CuratorTestBase;
import org.apache.curator.test.compatibility.Timing2;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class TestConnectionStateManager extends BaseClassForTests {

    @Test
    @Tag(CuratorTestBase.zk35TestCompatibilityGroup)
    public void testSessionConnectionStateErrorPolicyWithExpirationPercent30() throws Exception {
        Timing2 timing = new Timing2();
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(server.getConnectString())
                .connectionTimeoutMs(1000)
                .sessionTimeoutMs(timing.session())
                .retryPolicy(new RetryOneTime(1))
                .connectionStateErrorPolicy(new SessionConnectionStateErrorPolicy())
                .simulatedSessionExpirationPercent(30)
                .build();

        // we should get LOST around 30% of a session plus a little "slop" for processing, etc.
        final int lostStateExpectedMs =
                (timing.session() / 3) + timing.forSleepingABit().milliseconds();
        try {
            CountDownLatch connectedLatch = new CountDownLatch(1);
            CountDownLatch lostLatch = new CountDownLatch(1);
            ConnectionStateListener stateListener = new ConnectionStateListener() {
                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState) {
                    if (newState == ConnectionState.CONNECTED) {
                        connectedLatch.countDown();
                    }
                    if (newState == ConnectionState.LOST) {
                        lostLatch.countDown();
                    }
                }
            };

            timing.sleepABit();

            client.getConnectionStateListenable().addListener(stateListener);
            client.start();
            assertTrue(timing.awaitLatch(connectedLatch));
            server.close();

            assertTrue(lostLatch.await(lostStateExpectedMs, TimeUnit.MILLISECONDS));
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    @Tag(CuratorTestBase.zk36Group)
    public void testConnectionStateRecoversFromUnexpectedExpiredConnection() throws Exception {
        Timing2 timing = new Timing2();
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(server.getConnectString())
                .connectionTimeoutMs(1_000)
                .sessionTimeoutMs(250) // try to aggressively expire the connection
                .retryPolicy(new RetryOneTime(1))
                .connectionStateErrorPolicy(new SessionConnectionStateErrorPolicy())
                .build();
        final BlockingQueue<ConnectionState> queue = Queues.newLinkedBlockingQueue();
        ConnectionStateListener listener = (client1, state) -> queue.add(state);
        client.getConnectionStateListenable().addListener(listener);
        client.start();
        try {
            ConnectionState polled = queue.poll(timing.forWaiting().seconds(), TimeUnit.SECONDS);
            assertEquals(polled, ConnectionState.CONNECTED);
            client.getZookeeperClient()
                    .getZooKeeper()
                    .getTestable()
                    .queueEvent(new WatchedEvent(
                            Watcher.Event.EventType.None, Watcher.Event.KeeperState.Disconnected, null));
            polled = queue.poll(timing.forWaiting().seconds(), TimeUnit.SECONDS);
            assertEquals(polled, ConnectionState.SUSPENDED);
            assertThrows(RuntimeException.class, () -> client.getZookeeperClient()
                    .getZooKeeper()
                    .getTestable()
                    .queueEvent(
                            new WatchedEvent(Watcher.Event.EventType.None, Watcher.Event.KeeperState.Expired, null) {
                                @Override
                                public String getPath() {
                                    // exception will cause ZooKeeper to update current state but fail to notify
                                    // watchers
                                    throw new RuntimeException("Path doesn't exist!");
                                }
                            }));
            polled = queue.poll(timing.forWaiting().seconds(), TimeUnit.SECONDS);
            assertEquals(polled, ConnectionState.LOST);
            polled = queue.poll(timing.forWaiting().seconds(), TimeUnit.SECONDS);
            assertEquals(polled, ConnectionState.RECONNECTED);
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }
}
