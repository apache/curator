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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.WatcherRemoveCuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.Timing;
import org.apache.curator.test.WatchersDebug;
import org.apache.curator.test.compatibility.CuratorTestBase;
import org.apache.curator.utils.DebugUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.flexible.QuorumMaj;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestWatcherRemovalManager extends CuratorTestBase {
    private static final String superUserPasswordDigest = "curator-test:zghsj3JfJqK7DbWf0RQ1BgbJH9w="; // ran from
    private static final String superUserPassword = "curator-test";

    @BeforeEach
    @Override
    public void setup() throws Exception {
        System.setProperty("zookeeper.DigestAuthenticationProvider.superDigest", superUserPasswordDigest);
        super.setup();
    }

    @Test
    public void testSameWatcherDifferentPaths1Triggered() throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try {
            client.start();
            WatcherRemovalFacade removerClient = (WatcherRemovalFacade) client.newWatcherRemoveCuratorFramework();
            final CountDownLatch latch = new CountDownLatch(1);
            Watcher watcher = new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    latch.countDown();
                }
            };
            removerClient.checkExists().usingWatcher(watcher).forPath("/a/b/c");
            removerClient.checkExists().usingWatcher(watcher).forPath("/d/e/f");
            removerClient.create().creatingParentsIfNeeded().forPath("/d/e/f");

            Timing timing = new Timing();
            assertTrue(timing.awaitLatch(latch));
            timing.sleepABit();

            removerClient.removeWatchers();
        } finally {
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
    public void testSameWatcherDifferentPaths() throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try {
            client.start();
            WatcherRemovalFacade removerClient = (WatcherRemovalFacade) client.newWatcherRemoveCuratorFramework();
            Watcher watcher = new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    // NOP
                }
            };
            removerClient.checkExists().usingWatcher(watcher).forPath("/a/b/c");
            removerClient.checkExists().usingWatcher(watcher).forPath("/d/e/f");
            assertEquals(removerClient.getWatcherRemovalManager().getEntries().size(), 2);
            removerClient.removeWatchers();
        } finally {
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
    public void testSameWatcherDifferentKinds1Triggered() throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try {
            client.start();
            WatcherRemovalFacade removerClient = (WatcherRemovalFacade) client.newWatcherRemoveCuratorFramework();
            final CountDownLatch latch = new CountDownLatch(1);
            Watcher watcher = new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    latch.countDown();
                }
            };

            removerClient.create().creatingParentsIfNeeded().forPath("/a/b/c");
            removerClient.checkExists().usingWatcher(watcher).forPath("/a/b/c");
            removerClient.getData().usingWatcher(watcher).forPath("/a/b/c");
            removerClient.setData().forPath("/a/b/c", "new".getBytes());

            Timing timing = new Timing();
            assertTrue(timing.awaitLatch(latch));
            timing.sleepABit();

            removerClient.removeWatchers();
        } finally {
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
    public void testSameWatcherDifferentKinds() throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try {
            client.start();
            WatcherRemovalFacade removerClient = (WatcherRemovalFacade) client.newWatcherRemoveCuratorFramework();
            Watcher watcher = new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    // NOP
                }
            };

            removerClient.create().creatingParentsIfNeeded().forPath("/a/b/c");
            removerClient.checkExists().usingWatcher(watcher).forPath("/a/b/c");
            removerClient.getData().usingWatcher(watcher).forPath("/a/b/c");
            removerClient.removeWatchers();
        } finally {
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
    public void testWithRetry() throws Exception {
        server.stop();
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try {
            client.start();
            WatcherRemovalFacade removerClient = (WatcherRemovalFacade) client.newWatcherRemoveCuratorFramework();
            Watcher w = new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    // NOP
                }
            };
            try {
                removerClient.checkExists().usingWatcher(w).forPath("/one/two/three");
                fail("Should have thrown ConnectionLossException");
            } catch (KeeperException.ConnectionLossException expected) {
                // expected
            }
            assertEquals(removerClient.getWatcherRemovalManager().getEntries().size(), 0);
        } finally {
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
    public void testWithRetryInBackground() throws Exception {
        server.stop();
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try {
            client.start();
            WatcherRemovalFacade removerClient = (WatcherRemovalFacade) client.newWatcherRemoveCuratorFramework();
            Watcher w = new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    // NOP
                }
            };

            final CountDownLatch latch = new CountDownLatch(1);
            BackgroundCallback callback = new BackgroundCallback() {
                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                    latch.countDown();
                }
            };
            removerClient.checkExists().usingWatcher(w).inBackground(callback).forPath("/one/two/three");
            assertTrue(new Timing().awaitLatch(latch));
            assertEquals(removerClient.getWatcherRemovalManager().getEntries().size(), 0);
        } finally {
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
    public void testMissingNode() throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try {
            client.start();
            WatcherRemovalFacade removerClient = (WatcherRemovalFacade) client.newWatcherRemoveCuratorFramework();
            Watcher w = new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    // NOP
                }
            };
            try {
                removerClient.getData().usingWatcher(w).forPath("/one/two/three");
                fail("Should have thrown NoNodeException");
            } catch (KeeperException.NoNodeException expected) {
                // expected
            }
            removerClient.removeWatchers();
        } finally {
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
    public void testMissingNodeInBackground() throws Exception {
        final CuratorFramework client =
                CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        Callable<Void> proc = new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                client.start();
                WatcherRemovalFacade removerClient = (WatcherRemovalFacade) client.newWatcherRemoveCuratorFramework();
                Watcher w = new Watcher() {
                    @Override
                    public void process(WatchedEvent event) {
                        // NOP
                    }
                };
                final CountDownLatch latch = new CountDownLatch(1);
                BackgroundCallback callback = new BackgroundCallback() {
                    @Override
                    public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                        latch.countDown();
                    }
                };
                removerClient.getData().usingWatcher(w).inBackground(callback).forPath("/one/two/three");
                assertTrue(new Timing().awaitLatch(latch));
                assertEquals(
                        removerClient.getWatcherRemovalManager().getEntries().size(), 0);
                removerClient.removeWatchers();
                return null;
            }
        };
        TestCleanState.test(client, proc);
    }

    @Test
    public void testBasic() throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try {
            client.start();
            internalTryBasic(client);
        } finally {
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
    public void testBasicNamespace1() throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try {
            client.start();
            internalTryBasic(client.usingNamespace("foo"));
        } finally {
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
    public void testBasicNamespace2() throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(server.getConnectString())
                .retryPolicy(new RetryOneTime(1))
                .namespace("hey")
                .build();
        try {
            client.start();
            internalTryBasic(client);
        } finally {
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
    public void testBasicNamespace3() throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(server.getConnectString())
                .retryPolicy(new RetryOneTime(1))
                .namespace("hey")
                .build();
        try {
            client.start();
            internalTryBasic(client.usingNamespace("lakjsf"));
        } finally {
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
    public void testEnsembleTracker() throws Exception {
        // given: client with ensemble tracker
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(server.getConnectString())
                .retryPolicy(new RetryOneTime(1))
                .namespace("hey")
                .ensembleTracker(true)
                .authorization("digest", superUserPassword.getBytes())
                .build();
        try {
            client.start();

            // We are using standalone, so "/zookeeper/config" will be empty.
            // So let's set it directly.
            QuorumMaj quorumMaj = new QuorumMaj(Collections.singletonMap(
                    1L,
                    new QuorumPeer.QuorumServer(1, "127.0.0.1:2182:2183:participant;" + server.getConnectString())));
            quorumMaj.setVersion(1);
            client.usingNamespace(null)
                    .setData()
                    .forPath(ZooDefs.CONFIG_NODE, quorumMaj.toString().getBytes());

            // when: zookeeper config node data fetched
            while (client.getCurrentConfig().getVersion() == 0) {
                Thread.sleep(100);
            }

            // then: the watcher must be attached
            assertEquals(
                    1,
                    WatchersDebug.getDataWatches(client.getZookeeperClient().getZooKeeper())
                            .size());

            // when: ensemble tracker closed
            System.setProperty(DebugUtils.PROPERTY_REMOVE_WATCHERS_IN_FOREGROUND, "true");
            ((CuratorFrameworkImpl) client).getEnsembleTracker().close();

            // then: the watcher must be removed
            assertEquals(
                    0,
                    WatchersDebug.getDataWatches(client.getZookeeperClient().getZooKeeper())
                            .size());
        } finally {
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
    public void testSameWatcher() throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try {
            client.start();
            client.create().forPath("/test");

            WatcherRemovalFacade removerClient = (WatcherRemovalFacade) client.newWatcherRemoveCuratorFramework();

            Watcher watcher = new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    // NOP
                }
            };

            removerClient.getData().usingWatcher(watcher).forPath("/test");
            assertEquals(removerClient.getRemovalManager().getEntries().size(), 1);
            removerClient.getData().usingWatcher(watcher).forPath("/test");
            assertEquals(removerClient.getRemovalManager().getEntries().size(), 1);
            removerClient.removeWatchers();
        } finally {
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
    public void testTriggered() throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try {
            client.start();

            WatcherRemovalFacade removerClient = (WatcherRemovalFacade) client.newWatcherRemoveCuratorFramework();

            final CountDownLatch latch = new CountDownLatch(1);
            Watcher watcher = new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (event.getType() == Event.EventType.NodeCreated) {
                        latch.countDown();
                    }
                }
            };

            removerClient.checkExists().usingWatcher(watcher).forPath("/yo");
            assertEquals(removerClient.getRemovalManager().getEntries().size(), 1);
            removerClient.create().forPath("/yo");

            assertTrue(new Timing().awaitLatch(latch));

            assertEquals(removerClient.getRemovalManager().getEntries().size(), 0);
        } finally {
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
    public void testResetFromWatcher() throws Exception {
        Timing timing = new Timing();
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try {
            client.start();

            final WatcherRemovalFacade removerClient = (WatcherRemovalFacade) client.newWatcherRemoveCuratorFramework();

            final CountDownLatch createdLatch = new CountDownLatch(1);
            final CountDownLatch deletedLatch = new CountDownLatch(1);
            Watcher watcher = new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (event.getType() == Event.EventType.NodeCreated) {
                        try {
                            removerClient.checkExists().usingWatcher(this).forPath("/yo");
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        createdLatch.countDown();
                    } else if (event.getType() == Event.EventType.NodeDeleted) {
                        deletedLatch.countDown();
                    }
                }
            };

            removerClient.checkExists().usingWatcher(watcher).forPath("/yo");
            assertEquals(removerClient.getRemovalManager().getEntries().size(), 1);
            removerClient.create().forPath("/yo");

            assertTrue(timing.awaitLatch(createdLatch));
            assertEquals(removerClient.getRemovalManager().getEntries().size(), 1);

            removerClient.delete().forPath("/yo");

            assertTrue(timing.awaitLatch(deletedLatch));

            assertEquals(removerClient.getRemovalManager().getEntries().size(), 0);
        } finally {
            TestCleanState.closeAndTestClean(client);
        }
    }

    private void internalTryBasic(CuratorFramework client) throws Exception {
        WatcherRemoveCuratorFramework removerClient = client.newWatcherRemoveCuratorFramework();

        final CountDownLatch latch = new CountDownLatch(1);
        Watcher watcher = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getType() == Event.EventType.DataWatchRemoved) {
                    latch.countDown();
                }
            }
        };
        removerClient.checkExists().usingWatcher(watcher).forPath("/hey");

        List<String> existWatches =
                WatchersDebug.getExistWatches(client.getZookeeperClient().getZooKeeper());
        assertEquals(existWatches.size(), 1);

        removerClient.removeWatchers();

        assertTrue(new Timing().awaitLatch(latch));

        existWatches = WatchersDebug.getExistWatches(client.getZookeeperClient().getZooKeeper());
        assertEquals(existWatches.size(), 0);
    }
}
