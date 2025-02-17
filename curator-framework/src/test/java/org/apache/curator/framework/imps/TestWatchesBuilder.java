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

package org.apache.curator.framework.imps;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.imps.FailedRemoveWatchManager.FailedRemoveWatchDetails;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.Timing;
import org.apache.curator.test.compatibility.CuratorTestBase;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZookeeperFactory;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.WatcherType;
import org.apache.zookeeper.ZooKeeper;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class TestWatchesBuilder extends CuratorTestBase {
    private AtomicReference<ConnectionState> registerConnectionStateListener(CuratorFramework client) {
        final AtomicReference<ConnectionState> state = new AtomicReference<ConnectionState>();
        client.getConnectionStateListenable().addListener(new ConnectionStateListener() {

            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState) {
                state.set(newState);
                synchronized (state) {
                    state.notify();
                }
            }
        });

        return state;
    }

    private boolean blockUntilDesiredConnectionState(
            AtomicReference<ConnectionState> stateRef, Timing timing, final ConnectionState desiredState) {
        if (stateRef.get() == desiredState) {
            return true;
        }

        //noinspection SynchronizationOnLocalVariableOrMethodParameter
        synchronized (stateRef) {
            if (stateRef.get() == desiredState) {
                return true;
            }

            try {
                stateRef.wait(timing.milliseconds());
                return stateRef.get() == desiredState;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
    }

    @Test
    public void testRemoveCuratorDefaultWatcher() throws Exception {
        Timing timing = new Timing();
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(server.getConnectString())
                .retryPolicy(new RetryOneTime(1))
                .build();
        try {
            client.start();

            final CountDownLatch removedLatch = new CountDownLatch(1);

            final String path = "/";
            client.getCuratorListenable().addListener(new CuratorListener() {
                @Override
                public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
                    if (event.getType() == CuratorEventType.WATCHED
                            && event.getWatchedEvent().getType() == EventType.DataWatchRemoved) {
                        removedLatch.countDown();
                    }
                }
            });

            client.checkExists().watched().forPath(path);

            client.watches().removeAll().forPath(path);

            assertTrue(timing.awaitLatch(removedLatch), "Timed out waiting for watch removal");
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testRemoveCuratorWatch() throws Exception {
        Timing timing = new Timing();
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(server.getConnectString())
                .retryPolicy(new RetryOneTime(1))
                .build();
        try {
            client.start();

            final CountDownLatch removedLatch = new CountDownLatch(1);

            final String path = "/";
            CuratorWatcher watcher = new CuratorWatcher() {

                @Override
                public void process(WatchedEvent event) throws Exception {
                    if (event.getPath().equals(path) && event.getType() == EventType.DataWatchRemoved) {
                        removedLatch.countDown();
                    }
                }
            };

            client.checkExists().usingWatcher(watcher).forPath(path);

            client.watches().remove(watcher).forPath(path);

            assertTrue(timing.awaitLatch(removedLatch), "Timed out waiting for watch removal");
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testRemoveWatch() throws Exception {
        Timing timing = new Timing();
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(server.getConnectString())
                .retryPolicy(new RetryOneTime(1))
                .build();
        try {
            client.start();

            final CountDownLatch removedLatch = new CountDownLatch(1);

            final String path = "/";
            Watcher watcher = new CountDownWatcher(path, removedLatch, EventType.DataWatchRemoved);

            client.checkExists().usingWatcher(watcher).forPath(path);

            client.watches().remove(watcher).forPath(path);

            assertTrue(timing.awaitLatch(removedLatch), "Timed out waiting for watch removal");
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testRemoveWatchInBackgroundWithCallback() throws Exception {
        Timing timing = new Timing();
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(server.getConnectString())
                .retryPolicy(new RetryOneTime(1))
                .build();
        try {
            client.start();

            // Make sure that the event fires on both the watcher and the callback.
            final CountDownLatch removedLatch = new CountDownLatch(2);
            final String path = "/";
            Watcher watcher = new CountDownWatcher(path, removedLatch, EventType.DataWatchRemoved);

            BackgroundCallback callback = new BackgroundCallback() {

                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                    if (event.getType() == CuratorEventType.REMOVE_WATCHES
                            && event.getPath().equals(path)) {
                        removedLatch.countDown();
                    }
                }
            };

            client.checkExists().usingWatcher(watcher).forPath(path);

            client.watches()
                    .remove(watcher)
                    .ofType(WatcherType.Any)
                    .inBackground(callback)
                    .forPath(path);

            assertTrue(timing.awaitLatch(removedLatch), "Timed out waiting for watch removal");

        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testRemoveWatchInBackgroundWithNoCallback() throws Exception {
        Timing timing = new Timing();
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(server.getConnectString())
                .retryPolicy(new RetryOneTime(1))
                .build();
        try {
            client.start();

            final String path = "/";
            final CountDownLatch removedLatch = new CountDownLatch(1);
            Watcher watcher = new CountDownWatcher(path, removedLatch, EventType.DataWatchRemoved);

            client.checkExists().usingWatcher(watcher).forPath(path);

            client.watches().remove(watcher).inBackground().forPath(path);

            assertTrue(timing.awaitLatch(removedLatch), "Timed out waiting for watch removal");

        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testRemoveAllWatches() throws Exception {
        Timing timing = new Timing();
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(server.getConnectString())
                .retryPolicy(new RetryOneTime(1))
                .build();
        try {
            client.start();

            final String path = "/";
            final CountDownLatch removedLatch = new CountDownLatch(2);

            Watcher watcher1 = new CountDownWatcher(path, removedLatch, EventType.ChildWatchRemoved);
            Watcher watcher2 = new CountDownWatcher(path, removedLatch, EventType.DataWatchRemoved);

            client.getChildren().usingWatcher(watcher1).forPath(path);
            client.checkExists().usingWatcher(watcher2).forPath(path);

            client.watches().removeAll().forPath(path);

            assertTrue(timing.awaitLatch(removedLatch), "Timed out waiting for watch removal");
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testRemoveAllDataWatches() throws Exception {
        Timing timing = new Timing();
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(server.getConnectString())
                .retryPolicy(new RetryOneTime(1))
                .build();
        try {
            client.start();

            final String path = "/";
            final AtomicBoolean removedFlag = new AtomicBoolean(false);
            final CountDownLatch removedLatch = new CountDownLatch(1);

            Watcher watcher1 = new BooleanWatcher(path, removedFlag, EventType.ChildWatchRemoved);
            Watcher watcher2 = new CountDownWatcher(path, removedLatch, EventType.DataWatchRemoved);

            client.getChildren().usingWatcher(watcher1).forPath(path);
            client.checkExists().usingWatcher(watcher2).forPath(path);

            client.watches().removeAll().ofType(WatcherType.Data).forPath(path);

            assertTrue(timing.awaitLatch(removedLatch), "Timed out waiting for watch removal");
            assertEquals(removedFlag.get(), false);
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testRemoveAllChildWatches() throws Exception {
        Timing timing = new Timing();
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(server.getConnectString())
                .retryPolicy(new RetryOneTime(1))
                .build();
        try {
            client.start();

            final String path = "/";
            final AtomicBoolean removedFlag = new AtomicBoolean(false);
            final CountDownLatch removedLatch = new CountDownLatch(1);

            Watcher watcher1 = new BooleanWatcher(path, removedFlag, EventType.DataWatchRemoved);
            Watcher watcher2 = new CountDownWatcher(path, removedLatch, EventType.ChildWatchRemoved);

            client.checkExists().usingWatcher(watcher1).forPath(path);
            client.getChildren().usingWatcher(watcher2).forPath(path);

            client.watches().removeAll().ofType(WatcherType.Children).forPath(path);

            assertTrue(timing.awaitLatch(removedLatch), "Timed out waiting for watch removal");
            assertEquals(removedFlag.get(), false);
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testRemoveLocalWatch() throws Exception {
        Timing timing = new Timing();
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(server.getConnectString())
                .retryPolicy(new RetryOneTime(1))
                .build();
        try {
            client.start();

            AtomicReference<ConnectionState> stateRef = registerConnectionStateListener(client);

            final String path = "/";

            final CountDownLatch removedLatch = new CountDownLatch(1);

            Watcher watcher = new CountDownWatcher(path, removedLatch, EventType.DataWatchRemoved);

            client.checkExists().usingWatcher(watcher).forPath(path);

            // Stop the server so we can check if we can remove watches locally when offline
            server.stop();

            assertTrue(blockUntilDesiredConnectionState(stateRef, timing, ConnectionState.SUSPENDED));

            client.watches().removeAll().locally().forPath(path);

            assertTrue(timing.awaitLatch(removedLatch), "Timed out waiting for watch removal");
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testRemoveLocalWatchInBackground() throws Exception {
        Timing timing = new Timing();
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(server.getConnectString())
                .retryPolicy(new RetryOneTime(1))
                .build();
        try {
            client.start();

            AtomicReference<ConnectionState> stateRef = registerConnectionStateListener(client);

            final String path = "/";

            final CountDownLatch removedLatch = new CountDownLatch(1);

            Watcher watcher = new CountDownWatcher(path, removedLatch, EventType.DataWatchRemoved);

            client.checkExists().usingWatcher(watcher).forPath(path);

            // Stop the server so we can check if we can remove watches locally when offline
            server.stop();

            assertTrue(blockUntilDesiredConnectionState(stateRef, timing, ConnectionState.SUSPENDED));

            client.watches().removeAll().locally().inBackground().forPath(path);

            assertTrue(timing.awaitLatch(removedLatch), "Timed out waiting for watch removal");
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    /**
     * Test the case where we try and remove an unregistered watcher. In this case we expect a NoWatcherException to
     * be thrown.
     * @throws Exception
     */
    @Test
    public void testRemoveUnregisteredWatcher() throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(server.getConnectString())
                .retryPolicy(new RetryOneTime(1))
                .build();
        try {
            client.start();

            final String path = "/";
            Watcher watcher = new Watcher() {
                @Override
                public void process(WatchedEvent event) {}
            };

            try {
                client.watches().remove(watcher).forPath(path);
                fail("Expected KeeperException.NoWatcherException");
            } catch (KeeperException.NoWatcherException expected) {
                // expected
            }
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    /**
     * Test the case where we try and remove an unregistered watcher but have the quietly flag set. In this case we expect success.
     * @throws Exception
     */
    @Test
    public void testRemoveUnregisteredWatcherQuietly() throws Exception {
        Timing timing = new Timing();
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(server.getConnectString())
                .retryPolicy(new RetryOneTime(1))
                .build();
        try {
            client.start();

            final AtomicBoolean watcherRemoved = new AtomicBoolean(false);

            final String path = "/";
            Watcher watcher = new BooleanWatcher(path, watcherRemoved, EventType.DataWatchRemoved);

            client.watches().remove(watcher).quietly().forPath(path);

            timing.sleepABit();

            // There should be no watcher removed as none were registered.
            assertEquals(watcherRemoved.get(), false);
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testGuaranteedRemoveWatch() throws Exception {
        Timing timing = new Timing();
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(server.getConnectString())
                .retryPolicy(new RetryOneTime(1))
                .build();
        try {
            client.start();

            AtomicReference<ConnectionState> stateRef = registerConnectionStateListener(client);

            String path = "/";

            CountDownLatch removeLatch = new CountDownLatch(1);

            Watcher watcher = new CountDownWatcher(path, removeLatch, EventType.DataWatchRemoved);
            client.checkExists().usingWatcher(watcher).forPath(path);

            server.stop();

            assertTrue(blockUntilDesiredConnectionState(stateRef, timing, ConnectionState.SUSPENDED));

            // Remove the watch while we're not connected
            try {
                client.watches().remove(watcher).guaranteed().forPath(path);
                fail();
            } catch (KeeperException.ConnectionLossException e) {
                // Expected
            }

            server.restart();

            timing.awaitLatch(removeLatch);
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testGuaranteedRemoveWatchInBackground() throws Exception {
        Timing timing = new Timing();
        CuratorFramework client = CuratorFrameworkFactory.newClient(
                server.getConnectString(), timing.session(), timing.connection(), new ExponentialBackoffRetry(100, 3));
        try {
            client.start();

            AtomicReference<ConnectionState> stateRef = registerConnectionStateListener(client);

            final CountDownLatch guaranteeAddedLatch = new CountDownLatch(1);

            ((CuratorFrameworkBase) client).getFailedRemoveWatcherManager().debugListener =
                    new FailedOperationManager.FailedOperationManagerListener<
                            FailedRemoveWatchManager.FailedRemoveWatchDetails>() {

                        @Override
                        public void pathAddedForGuaranteedOperation(FailedRemoveWatchDetails detail) {
                            guaranteeAddedLatch.countDown();
                        }
                    };

            String path = "/";

            CountDownLatch removeLatch = new CountDownLatch(1);

            Watcher watcher = new CountDownWatcher(path, removeLatch, EventType.DataWatchRemoved);
            client.checkExists().usingWatcher(watcher).forPath(path);

            server.stop();
            assertTrue(blockUntilDesiredConnectionState(stateRef, timing, ConnectionState.SUSPENDED));

            // Remove the watch while we're not connected
            client.watches().remove(watcher).guaranteed().inBackground().forPath(path);

            timing.awaitLatch(guaranteeAddedLatch);

            server.restart();

            timing.awaitLatch(removeLatch);
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    @Tag(CuratorTestBase.zk36Group)
    public void testPersistentWatch() throws Exception {
        try (CuratorFramework client =
                CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1))) {
            client.start();
            client.blockUntilConnected();

            CountDownLatch latch = new CountDownLatch(3);
            Watcher watcher = event -> latch.countDown();
            client.watchers()
                    .add()
                    .withMode(AddWatchMode.PERSISTENT)
                    .usingWatcher(watcher)
                    .forPath("/test/foo");

            client.create().creatingParentsIfNeeded().forPath("/test/foo");
            client.setData().forPath("/test/foo", "hey".getBytes());
            client.delete().forPath("/test/foo");

            assertTrue(timing.awaitLatch(latch));
        }
    }

    @Test
    @Tag(CuratorTestBase.zk36Group)
    public void testPersistentWatchInBackground() throws Exception {
        try (CuratorFramework client =
                CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1))) {
            client.start();
            client.blockUntilConnected();

            CountDownLatch backgroundLatch = new CountDownLatch(1);
            BackgroundCallback backgroundCallback = (__, ___) -> backgroundLatch.countDown();
            CountDownLatch latch = new CountDownLatch(3);
            Watcher watcher = event -> latch.countDown();
            client.watchers()
                    .add()
                    .withMode(AddWatchMode.PERSISTENT)
                    .inBackground(backgroundCallback)
                    .usingWatcher(watcher)
                    .forPath("/test/foo");

            client.create().creatingParentsIfNeeded().forPath("/test/foo");
            client.setData().forPath("/test/foo", "hey".getBytes());
            client.delete().forPath("/test/foo");

            assertTrue(timing.awaitLatch(backgroundLatch));
            assertTrue(timing.awaitLatch(latch));
        }
    }

    @Test
    @Tag(CuratorTestBase.zk36Group)
    public void testPersistentRecursiveWatch() throws Exception {
        try (CuratorFramework client =
                CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1))) {
            client.start();
            client.blockUntilConnected();

            CountDownLatch latch = new CountDownLatch(5);
            Watcher watcher = event -> latch.countDown();
            client.watchers()
                    .add()
                    .withMode(AddWatchMode.PERSISTENT_RECURSIVE)
                    .usingWatcher(watcher)
                    .forPath("/test");

            client.create().forPath("/test");
            client.create().forPath("/test/a");
            client.create().forPath("/test/a/b");
            client.create().forPath("/test/a/b/c");
            client.create().forPath("/test/a/b/c/d");

            assertTrue(timing.awaitLatch(latch));
        }
    }

    @Test
    @Tag(CuratorTestBase.zk36Group)
    public void testPersistentRecursiveWatchInBackground() throws Exception {
        try (CuratorFramework client =
                CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1))) {
            client.start();
            client.blockUntilConnected();

            CountDownLatch backgroundLatch = new CountDownLatch(1);
            BackgroundCallback backgroundCallback = (__, ___) -> backgroundLatch.countDown();
            CountDownLatch latch = new CountDownLatch(5);
            Watcher watcher = event -> latch.countDown();
            client.watchers()
                    .add()
                    .withMode(AddWatchMode.PERSISTENT_RECURSIVE)
                    .inBackground(backgroundCallback)
                    .usingWatcher(watcher)
                    .forPath("/test");

            client.create().forPath("/test");
            client.create().forPath("/test/a");
            client.create().forPath("/test/a/b");
            client.create().forPath("/test/a/b/c");
            client.create().forPath("/test/a/b/c/d");

            assertTrue(timing.awaitLatch(backgroundLatch));
            assertTrue(timing.awaitLatch(latch));
        }
    }

    @Test
    @Tag(CuratorTestBase.zk36Group)
    public void testPersistentRecursiveDefaultWatch() throws Exception {
        CountDownLatch latch = new CountDownLatch(6); // 5 creates plus the initial sync
        ZookeeperFactory zookeeperFactory = (connectString, sessionTimeout, watcher, canBeReadOnly) -> {
            Watcher actualWatcher = event -> {
                watcher.process(event);
                latch.countDown();
            };
            return new ZooKeeper(connectString, sessionTimeout, actualWatcher);
        };
        try (CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(server.getConnectString())
                .retryPolicy(new RetryOneTime(1))
                .zookeeperFactory(zookeeperFactory)
                .build()) {
            client.start();
            client.blockUntilConnected();

            client.watchers().add().withMode(AddWatchMode.PERSISTENT_RECURSIVE).forPath("/test");

            client.create().forPath("/test");
            client.create().forPath("/test/a");
            client.create().forPath("/test/a/b");
            client.create().forPath("/test/a/b/c");
            client.create().forPath("/test/a/b/c/d");

            assertTrue(timing.awaitLatch(latch));
        }
    }

    private static class CountDownWatcher implements Watcher {
        private String path;
        private EventType eventType;
        private CountDownLatch removeLatch;

        public CountDownWatcher(String path, CountDownLatch removeLatch, EventType eventType) {
            this.path = path;
            this.eventType = eventType;
            this.removeLatch = removeLatch;
        }

        @Override
        public void process(WatchedEvent event) {
            if (event.getPath() == null || event.getType() == null) {
                return;
            }

            if (event.getPath().equals(path) && event.getType() == eventType) {
                removeLatch.countDown();
            }
        }
    }

    private static class BooleanWatcher implements Watcher {
        private String path;
        private EventType eventType;
        private AtomicBoolean removedFlag;

        public BooleanWatcher(String path, AtomicBoolean removedFlag, EventType eventType) {
            this.path = path;
            this.eventType = eventType;
            this.removedFlag = removedFlag;
        }

        @Override
        public void process(WatchedEvent event) {
            if (event.getPath() == null || event.getType() == null) {
                return;
            }

            if (event.getPath().equals(path) && event.getType() == eventType) {
                removedFlag.set(true);
            }
        }
    }
}
