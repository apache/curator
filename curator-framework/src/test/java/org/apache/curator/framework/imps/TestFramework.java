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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.curator.RetryPolicy;
import org.apache.curator.RetrySleeper;
import org.apache.curator.framework.AuthInfo;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CreateBuilder;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.api.DeleteBuilder;
import org.apache.curator.framework.api.ExistsBuilder;
import org.apache.curator.framework.api.GetACLBuilder;
import org.apache.curator.framework.api.GetChildrenBuilder;
import org.apache.curator.framework.api.GetDataBuilder;
import org.apache.curator.framework.api.SetACLBuilder;
import org.apache.curator.framework.api.SetDataBuilder;
import org.apache.curator.framework.api.transaction.CuratorMultiTransaction;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.Timing;
import org.apache.curator.test.compatibility.CuratorTestBase;
import org.apache.curator.test.compatibility.Timing2;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.EnsurePath;
import org.apache.curator.utils.ZKPaths;
import org.apache.curator.utils.ZookeeperFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("deprecation")
@Tag(CuratorTestBase.zk35TestCompatibilityGroup)
public class TestFramework extends BaseClassForTests {
    private final Logger log = LoggerFactory.getLogger(getClass());

    private static final String superUserPasswordDigest = "curator-test:zghsj3JfJqK7DbWf0RQ1BgbJH9w="; // ran from
    // DigestAuthenticationProvider.generateDigest(superUserPassword);
    private static final String superUserPassword = "curator-test";

    @BeforeEach
    @Override
    public void setup() throws Exception {
        System.setProperty("znode.container.checkIntervalMs", "1000");
        QuorumPeerConfig.setReconfigEnabled(true);
        System.setProperty("zookeeper.DigestAuthenticationProvider.superDigest", superUserPasswordDigest);
        super.setup();
    }

    @AfterEach
    @Override
    public void teardown() throws Exception {
        System.clearProperty("znode.container.checkIntervalMs");
        super.teardown();
    }

    public void testWaitForShutdownTimeoutMs() throws Exception {
        final BlockingQueue<Integer> timeoutQueue = new ArrayBlockingQueue<>(1);
        ZookeeperFactory zookeeperFactory = new ZookeeperFactory() {
            @Override
            public ZooKeeper newZooKeeper(
                    String connectString, int sessionTimeout, Watcher watcher, boolean canBeReadOnly)
                    throws IOException {
                return new ZooKeeper(connectString, sessionTimeout, watcher, canBeReadOnly) {
                    @Override
                    public boolean close(int waitForShutdownTimeoutMs) throws InterruptedException {
                        timeoutQueue.add(waitForShutdownTimeoutMs);
                        return super.close(waitForShutdownTimeoutMs);
                    }
                };
            }
        };

        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(server.getConnectString())
                .retryPolicy(new RetryOneTime(1))
                .zookeeperFactory(zookeeperFactory)
                .waitForShutdownTimeoutMs(10064)
                .build();
        try {
            client.start();
            client.checkExists().forPath("/foo");
        } finally {
            CloseableUtils.closeQuietly(client);
        }

        Integer polledValue = timeoutQueue.poll(new Timing().milliseconds(), TimeUnit.MILLISECONDS);
        assertNotNull(polledValue);
        assertEquals(10064, polledValue.intValue());
    }

    @Test
    public void testSessionLossWithLongTimeout() throws Exception {
        final Timing timing = new Timing();

        try (final CuratorFramework client = CuratorFrameworkFactory.newClient(
                server.getConnectString(),
                timing.forWaiting().milliseconds(),
                timing.connection(),
                new RetryOneTime(1))) {
            final CountDownLatch connectedLatch = new CountDownLatch(1);
            final CountDownLatch lostLatch = new CountDownLatch(1);
            final CountDownLatch restartedLatch = new CountDownLatch(1);
            client.getConnectionStateListenable().addListener(new ConnectionStateListener() {
                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState) {
                    if (newState == ConnectionState.CONNECTED) {
                        connectedLatch.countDown();
                    } else if (newState == ConnectionState.LOST) {
                        lostLatch.countDown();
                    } else if (newState == ConnectionState.RECONNECTED) {
                        restartedLatch.countDown();
                    }
                }
            });

            client.start();

            assertTrue(timing.awaitLatch(connectedLatch));

            server.stop();

            timing.sleepABit();
            assertTrue(timing.awaitLatch(lostLatch));

            server.restart();
            assertTrue(timing.awaitLatch(restartedLatch));
        }
    }

    @Test
    public void testConnectionState() throws Exception {
        Timing timing = new Timing();
        CuratorFramework client = CuratorFrameworkFactory.newClient(
                server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        try {
            final BlockingQueue<ConnectionState> queue = new LinkedBlockingQueue<ConnectionState>();
            ConnectionStateListener listener = new ConnectionStateListener() {
                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState) {
                    queue.add(newState);
                }
            };
            client.getConnectionStateListenable().addListener(listener);

            client.start();
            assertEquals(queue.poll(timing.multiple(4).seconds(), TimeUnit.SECONDS), ConnectionState.CONNECTED);
            server.stop();
            assertEquals(queue.poll(timing.multiple(4).seconds(), TimeUnit.SECONDS), ConnectionState.SUSPENDED);
            assertEquals(queue.poll(timing.multiple(4).seconds(), TimeUnit.SECONDS), ConnectionState.LOST);
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testCreateOrSetData() throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try {
            client.start();

            String name = client.create().forPath("/hey", "there".getBytes());
            assertEquals(name, "/hey");
            name = client.create().orSetData().forPath("/hey", "other".getBytes());
            assertEquals(name, "/hey");
            assertArrayEquals(client.getData().forPath("/hey"), "other".getBytes());

            name = client.create().orSetData().creatingParentsIfNeeded().forPath("/a/b/c", "there".getBytes());
            assertEquals(name, "/a/b/c");
            name = client.create().orSetData().creatingParentsIfNeeded().forPath("/a/b/c", "what".getBytes());
            assertEquals(name, "/a/b/c");
            assertArrayEquals(client.getData().forPath("/a/b/c"), "what".getBytes());

            final BlockingQueue<CuratorEvent> queue = new LinkedBlockingQueue<>();
            BackgroundCallback backgroundCallback = new BackgroundCallback() {
                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                    queue.add(event);
                }
            };
            client.create().orSetData().inBackground(backgroundCallback).forPath("/a/b/c", "another".getBytes());

            CuratorEvent event = queue.poll(new Timing().milliseconds(), TimeUnit.MILLISECONDS);
            assertNotNull(event);
            assertEquals(event.getResultCode(), KeeperException.Code.OK.intValue());
            assertEquals(event.getType(), CuratorEventType.CREATE);
            assertEquals(event.getPath(), "/a/b/c");
            assertEquals(event.getName(), "/a/b/c");

            // callback should only be called once
            CuratorEvent unexpectedEvent = queue.poll(new Timing().milliseconds(), TimeUnit.MILLISECONDS);
            assertNull(unexpectedEvent);
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testNamespaceWithWatcher() throws Exception {
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        CuratorFramework client = builder.connectString(server.getConnectString())
                .namespace("aisa")
                .retryPolicy(new RetryOneTime(1))
                .build();
        client.start();
        try {
            final BlockingQueue<String> queue = new LinkedBlockingQueue<String>();
            Watcher watcher = new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    try {
                        queue.put(event.getPath());
                    } catch (InterruptedException e) {
                        throw new Error(e);
                    }
                }
            };
            client.create().forPath("/base");
            client.getChildren().usingWatcher(watcher).forPath("/base");
            client.create().forPath("/base/child");

            String path = new Timing2().takeFromQueue(queue);
            assertEquals(path, "/base");
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testNamespaceInBackground() throws Exception {
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        CuratorFramework client = builder.connectString(server.getConnectString())
                .namespace("aisa")
                .retryPolicy(new RetryOneTime(1))
                .build();
        client.start();
        try {
            final BlockingQueue<String> queue = new LinkedBlockingQueue<String>();
            CuratorListener listener = new CuratorListener() {
                @Override
                public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
                    if (event.getType() == CuratorEventType.EXISTS) {
                        queue.put(event.getPath());
                    }
                }
            };

            client.getCuratorListenable().addListener(listener);
            client.create().forPath("/base");
            client.checkExists().inBackground().forPath("/base");

            String path = queue.poll(10, TimeUnit.SECONDS);
            assertEquals(path, "/base");

            client.getCuratorListenable().removeListener(listener);

            BackgroundCallback callback = new BackgroundCallback() {
                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                    queue.put(event.getPath());
                }
            };
            client.getChildren().inBackground(callback).forPath("/base");
            path = queue.poll(10, TimeUnit.SECONDS);
            assertEquals(path, "/base");
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testCreateACLSingleAuth() throws Exception {
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        CuratorFramework client = builder.connectString(server.getConnectString())
                .authorization("digest", "me1:pass1".getBytes())
                .retryPolicy(new RetryOneTime(1))
                .build();
        client.start();
        try {
            ACL acl = new ACL(ZooDefs.Perms.WRITE, ZooDefs.Ids.AUTH_IDS);
            List<ACL> aclList = Lists.newArrayList(acl);
            client.create().withACL(aclList).forPath("/test", "test".getBytes());
            client.close();

            // Try setting data with me1:pass1
            client = builder.connectString(server.getConnectString())
                    .authorization("digest", "me1:pass1".getBytes())
                    .retryPolicy(new RetryOneTime(1))
                    .build();
            client.start();
            try {
                client.setData().forPath("/test", "test".getBytes());
            } catch (KeeperException.NoAuthException e) {
                fail("Auth failed");
            }
            client.close();

            // Try setting data with something:else
            client = builder.connectString(server.getConnectString())
                    .authorization("digest", "something:else".getBytes())
                    .retryPolicy(new RetryOneTime(1))
                    .build();
            client.start();
            try {
                client.setData().forPath("/test", "test".getBytes());
                fail("Should have failed with auth exception");
            } catch (KeeperException.NoAuthException e) {
                // expected
            }
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testACLDeprecatedApis() throws Exception {
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder()
                .connectString(server.getConnectString())
                .retryPolicy(new RetryOneTime(1));
        assertNull(builder.getAuthScheme());
        assertNull(builder.getAuthValue());

        builder = builder.authorization("digest", "me1:pass1".getBytes());
        assertEquals(builder.getAuthScheme(), "digest");
        assertArrayEquals(builder.getAuthValue(), "me1:pass1".getBytes());
    }

    @Test
    public void testCreateACLMultipleAuths() throws Exception {
        // Add a few authInfos
        List<AuthInfo> authInfos = new ArrayList<AuthInfo>();
        authInfos.add(new AuthInfo("digest", "me1:pass1".getBytes()));
        authInfos.add(new AuthInfo("digest", "me2:pass2".getBytes()));

        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        CuratorFramework client = builder.connectString(server.getConnectString())
                .authorization(authInfos)
                .retryPolicy(new RetryOneTime(1))
                .build();
        client.start();
        try {
            ACL acl = new ACL(ZooDefs.Perms.WRITE, ZooDefs.Ids.AUTH_IDS);
            List<ACL> aclList = Lists.newArrayList(acl);
            client.create().withACL(aclList).forPath("/test", "test".getBytes());
            client.close();

            // Try setting data with me1:pass1
            client = builder.connectString(server.getConnectString())
                    .authorization("digest", "me1:pass1".getBytes())
                    .retryPolicy(new RetryOneTime(1))
                    .build();
            client.start();
            try {
                client.setData().forPath("/test", "test".getBytes());
            } catch (KeeperException.NoAuthException e) {
                fail("Auth failed");
            }
            client.close();

            // Try setting data with me1:pass1
            client = builder.connectString(server.getConnectString())
                    .authorization("digest", "me2:pass2".getBytes())
                    .retryPolicy(new RetryOneTime(1))
                    .build();
            client.start();
            try {
                client.setData().forPath("/test", "test".getBytes());
            } catch (KeeperException.NoAuthException e) {
                fail("Auth failed");
            }
            client.close();

            // Try setting data with something:else
            client = builder.connectString(server.getConnectString())
                    .authorization("digest", "something:else".getBytes())
                    .retryPolicy(new RetryOneTime(1))
                    .build();
            client.start();
            try {
                client.setData().forPath("/test", "test".getBytes());
                fail("Should have failed with auth exception");
            } catch (KeeperException.NoAuthException e) {
                // expected
            }
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testCreateACLWithReset() throws Exception {
        Timing timing = new Timing();
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        CuratorFramework client = builder.connectString(server.getConnectString())
                .sessionTimeoutMs(timing.session())
                .connectionTimeoutMs(timing.connection())
                .authorization("digest", "me:pass".getBytes())
                .retryPolicy(new RetryOneTime(1))
                .build();
        client.start();
        try {
            final CountDownLatch lostLatch = new CountDownLatch(1);
            ConnectionStateListener listener = new ConnectionStateListener() {
                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState) {
                    if (newState == ConnectionState.LOST) {
                        lostLatch.countDown();
                    }
                }
            };
            client.getConnectionStateListenable().addListener(listener);

            ACL acl = new ACL(ZooDefs.Perms.WRITE, ZooDefs.Ids.AUTH_IDS);
            List<ACL> aclList = Lists.newArrayList(acl);
            client.create().withACL(aclList).forPath("/test", "test".getBytes());

            server.stop();
            assertTrue(timing.awaitLatch(lostLatch));
            try {
                client.checkExists().forPath("/");
                fail("Connection should be down");
            } catch (KeeperException.ConnectionLossException e) {
                // expected
            }

            server.restart();
            try {
                client.setData().forPath("/test", "test".getBytes());
            } catch (KeeperException.NoAuthException e) {
                fail("Auth failed");
            }
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testCreateParents() throws Exception {
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        CuratorFramework client = builder.connectString(server.getConnectString())
                .retryPolicy(new RetryOneTime(1))
                .build();
        client.start();
        try {
            client.create().creatingParentsIfNeeded().forPath("/one/two/three", "foo".getBytes());
            byte[] data = client.getData().forPath("/one/two/three");
            assertArrayEquals(data, "foo".getBytes());

            client.create().creatingParentsIfNeeded().forPath("/one/two/another", "bar".getBytes());
            data = client.getData().forPath("/one/two/another");
            assertArrayEquals(data, "bar".getBytes());
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testOverrideCreateParentContainers() throws Exception {
        if (!checkForContainers()) {
            return;
        }

        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(server.getConnectString())
                .retryPolicy(new RetryOneTime(1))
                .dontUseContainerParents()
                .build();
        try {
            client.start();
            client.create().creatingParentContainersIfNeeded().forPath("/one/two/three", "foo".getBytes());
            byte[] data = client.getData().forPath("/one/two/three");
            assertArrayEquals(data, "foo".getBytes());

            client.delete().forPath("/one/two/three");
            new Timing().sleepABit();

            assertNotNull(client.checkExists().forPath("/one/two"));
            new Timing().sleepABit();
            assertNotNull(client.checkExists().forPath("/one"));
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testCreateParentContainers() throws Exception {
        if (!checkForContainers()) {
            return;
        }

        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        CuratorFramework client = builder.connectString(server.getConnectString())
                .retryPolicy(new RetryOneTime(1))
                .build();
        try {
            client.start();
            client.create().creatingParentContainersIfNeeded().forPath("/one/two/three", "foo".getBytes());
            byte[] data = client.getData().forPath("/one/two/three");
            assertArrayEquals(data, "foo".getBytes());

            client.delete().forPath("/one/two/three");
            new Timing().sleepABit();

            assertNull(client.checkExists().forPath("/one/two"));
            new Timing().sleepABit();
            assertNull(client.checkExists().forPath("/one"));
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    private boolean checkForContainers() {
        if (ZKPaths.getContainerCreateMode() == CreateMode.PERSISTENT) {
            System.out.println("Not using CreateMode.CONTAINER enabled version of ZooKeeper");
            return false;
        }
        return true;
    }

    @Test
    public void testCreatingParentsTheSame() throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try {
            client.start();

            assertNull(client.checkExists().forPath("/one/two"));
            client.create().creatingParentContainersIfNeeded().forPath("/one/two/three");
            assertNotNull(client.checkExists().forPath("/one/two"));

            client.delete().deletingChildrenIfNeeded().forPath("/one");
            assertNull(client.checkExists().forPath("/one"));

            assertNull(client.checkExists().forPath("/one/two"));
            client.checkExists().creatingParentContainersIfNeeded().forPath("/one/two/three");
            assertNotNull(client.checkExists().forPath("/one/two"));
            assertNull(client.checkExists().forPath("/one/two/three"));
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testExistsCreatingParents() throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try {
            client.start();

            assertNull(client.checkExists().forPath("/one/two"));
            client.checkExists().creatingParentContainersIfNeeded().forPath("/one/two/three");
            assertNotNull(client.checkExists().forPath("/one/two"));
            assertNull(client.checkExists().forPath("/one/two/three"));
            assertNull(client.checkExists().creatingParentContainersIfNeeded().forPath("/one/two/three"));
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testExistsCreatingParentsInBackground() throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try {
            client.start();

            assertNull(client.checkExists().forPath("/one/two"));

            final CountDownLatch latch = new CountDownLatch(1);
            BackgroundCallback callback = new BackgroundCallback() {
                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                    latch.countDown();
                }
            };
            client.checkExists()
                    .creatingParentContainersIfNeeded()
                    .inBackground(callback)
                    .forPath("/one/two/three");
            assertTrue(new Timing().awaitLatch(latch));
            assertNotNull(client.checkExists().forPath("/one/two"));
            assertNull(client.checkExists().forPath("/one/two/three"));
            assertNull(client.checkExists().creatingParentContainersIfNeeded().forPath("/one/two/three"));
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testEnsurePathWithNamespace() throws Exception {
        final String namespace = "jz";

        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        CuratorFramework client = builder.connectString(server.getConnectString())
                .retryPolicy(new RetryOneTime(1))
                .namespace(namespace)
                .build();
        client.start();
        try {
            EnsurePath ensurePath = new EnsurePath("/pity/the/fool");
            ensurePath.ensure(client.getZookeeperClient());
            assertNull(client.getZookeeperClient().getZooKeeper().exists("/jz/pity/the/fool", false));

            ensurePath = client.newNamespaceAwareEnsurePath("/pity/the/fool");
            ensurePath.ensure(client.getZookeeperClient());
            assertNotNull(client.getZookeeperClient().getZooKeeper().exists("/jz/pity/the/fool", false));
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testCreateContainersWithNamespace() throws Exception {
        final String namespace = "container1";
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        CuratorFramework client = builder.connectString(server.getConnectString())
                .retryPolicy(new RetryOneTime(1))
                .namespace(namespace)
                .build();
        try {
            client.start();
            String path = "/path1/path2";
            client.createContainers(path);
            assertNotNull(client.checkExists().forPath(path));
            assertNotNull(client.getZookeeperClient().getZooKeeper().exists("/" + namespace + path, false));
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testCreateContainersUsingNamespace() throws Exception {
        final String namespace = "container2";
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        CuratorFramework client = builder.connectString(server.getConnectString())
                .retryPolicy(new RetryOneTime(1))
                .build();
        try {
            client.start();
            CuratorFramework nsClient = client.usingNamespace(namespace);
            String path = "/path1/path2";
            nsClient.createContainers(path);
            assertNotNull(nsClient.checkExists().forPath(path));
            assertNotNull(nsClient.getZookeeperClient().getZooKeeper().exists("/" + namespace + path, false));
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testNamespace() throws Exception {
        final String namespace = "TestNamespace";

        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        CuratorFramework client = builder.connectString(server.getConnectString())
                .retryPolicy(new RetryOneTime(1))
                .namespace(namespace)
                .build();
        client.start();
        try {
            String actualPath = client.create().forPath("/test");
            assertEquals(actualPath, "/test");
            assertNotNull(client.getZookeeperClient().getZooKeeper().exists("/" + namespace + "/test", false));
            assertNull(client.getZookeeperClient().getZooKeeper().exists("/test", false));

            actualPath = client.usingNamespace(null).create().forPath("/non");
            assertEquals(actualPath, "/non");
            assertNotNull(client.getZookeeperClient().getZooKeeper().exists("/non", false));

            client.create().forPath("/test/child", "hey".getBytes());
            byte[] bytes = client.getData().forPath("/test/child");
            assertArrayEquals(bytes, "hey".getBytes());

            bytes = client.usingNamespace(null).getData().forPath("/" + namespace + "/test/child");
            assertArrayEquals(bytes, "hey".getBytes());
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testCustomCallback() throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try {
            final CountDownLatch latch = new CountDownLatch(1);
            BackgroundCallback callback = new BackgroundCallback() {
                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                    if (event.getType() == CuratorEventType.CREATE) {
                        if (event.getPath().equals("/head")) {
                            latch.countDown();
                        }
                    }
                }
            };
            client.create().inBackground(callback).forPath("/head");
            assertTrue(latch.await(10, TimeUnit.SECONDS));
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testSync() throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try {
            client.getCuratorListenable().addListener(new CuratorListener() {
                @Override
                public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
                    if (event.getType() == CuratorEventType.SYNC) {
                        assertEquals(event.getPath(), "/head");
                        ((CountDownLatch) event.getContext()).countDown();
                    }
                }
            });

            client.create().forPath("/head");
            assertNotNull(client.checkExists().forPath("/head"));

            CountDownLatch latch = new CountDownLatch(1);
            client.sync("/head", latch);
            assertTrue(latch.await(10, TimeUnit.SECONDS));
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testSyncNew() throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try {
            client.create().forPath("/head");
            assertNotNull(client.checkExists().forPath("/head"));

            final CountDownLatch latch = new CountDownLatch(1);
            BackgroundCallback callback = new BackgroundCallback() {
                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                    if (event.getType() == CuratorEventType.SYNC) {
                        latch.countDown();
                    }
                }
            };
            client.sync().inBackground(callback).forPath("/head");
            assertTrue(latch.await(10, TimeUnit.SECONDS));
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testGetSequentialChildren() throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try {
            client.create().forPath("/head");

            for (int i = 0; i < 10; ++i) {
                client.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath("/head/child");
            }

            List<String> children = client.getChildren().forPath("/head");
            assertEquals(children.size(), 10);
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    private static class AlwaysRetry implements RetryPolicy {
        private final int retryIntervalMs;

        public AlwaysRetry(int retryIntervalMs) {
            this.retryIntervalMs = retryIntervalMs;
        }

        @Override
        public boolean allowRetry(Throwable exception) {
            return exception instanceof KeeperException;
        }

        @Override
        public boolean allowRetry(int retryCount, long elapsedTimeMs, RetrySleeper retrySleeper) {
            try {
                retrySleeper.sleepFor(retryIntervalMs, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
            return true;
        }
    }

    /**
     * Block until curator fully stopped to test operations initiated before stopped but running after fully stopped.
     */
    private static class BlockUntilFullyStopped implements CuratorFrameworkImpl.DebugBackgroundListener {
        private final CuratorFramework client;
        private final BackgroundOperation<?> operation;
        private final long maxRuns;
        private long runs = 0;

        public BlockUntilFullyStopped(CuratorFramework client, BackgroundOperation<?> operation, long maxRuns) {
            this.client = client;
            this.operation = operation;
            this.maxRuns = maxRuns;
        }

        @Override
        public void listen(OperationAndData<?> data) {
            if (operation != data.getOperation()) {
                return;
            }
            runs++;
            if (runs > maxRuns) {
                while (!(client.getState() == CuratorFrameworkState.STOPPED
                        && !client.getZookeeperClient().isConnected())) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException ignored) {
                    }
                }
            }
        }
    }

    private interface BackgroundOperationFactory {
        BackgroundOperation<?> create(CuratorFramework client, CompletableFuture<CuratorEvent> future) throws Exception;
    }

    private void testBackgroundOperationWithConcurrentCloseAndChaosStalls(
            BackgroundOperationFactory operationFactory, long maxRuns, long[] millisStalls) throws Exception {
        AlwaysRetry alwaysRetry = new AlwaysRetry(2);
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), alwaysRetry);
        client.start();
        try {
            // given: error background request with always-retry policy
            CompletableFuture<CuratorEvent> future = new CompletableFuture<>();
            BackgroundOperation<?> operation = operationFactory.create(client, future);

            // These chaos steps create chances to run into concurrent contentions.
            // They could fail this test given enough runs if there are bugs.
            if (maxRuns > 0) {
                ((CuratorFrameworkImpl) client).debugListener = new BlockUntilFullyStopped(client, operation, maxRuns);
            }
            for (long ms : millisStalls) {
                if (ms >= 0) {
                    Thread.sleep(ms);
                } else {
                    restartServer();
                }
            }

            // when: close client while operation is queuing, retrying or awaking from sleep
            client.close();

            // then: get closing event with session expired error
            CuratorEvent event = future.get(10, TimeUnit.SECONDS);
            assertThat(event.getResultCode()).isEqualTo(KeeperException.Code.SESSIONEXPIRED.intValue());
            assertThat(event.getType()).isSameAs(operation.getBackgroundEventType());
            assertThat(event.getContext()).isSameAs(future);
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    private void testBackgroundOperationWithConcurrentClose(BackgroundOperationFactory operationFactory)
            throws Exception {
        testBackgroundOperationWithConcurrentCloseAndChaosStalls(operationFactory, -1, new long[] {20, -1, 5});
        testBackgroundOperationWithConcurrentCloseAndChaosStalls(operationFactory, -1, new long[] {10});
        testBackgroundOperationWithConcurrentCloseAndChaosStalls(operationFactory, 2, new long[] {20});
    }

    @Test
    public void testBackgroundCreateWithConcurrentClose() throws Exception {
        AtomicBoolean retry = new AtomicBoolean();
        testBackgroundOperationWithConcurrentClose((client, future) -> {
            if (retry.compareAndSet(false, true)) {
                try {
                    client.create().forPath("/exist-path");
                } catch (KeeperException ex) {
                    throw new IllegalStateException(ex);
                }
            }
            CreateBuilder create = client.create();
            create.inBackground((ignored, event) -> future.complete(event), future)
                    .forPath("/exist-path");
            return (BackgroundOperation<?>) create;
        });
    }

    @Test
    public void testBackgroundDeleteWithConcurrentClose() throws Exception {
        testBackgroundOperationWithConcurrentClose((client, future) -> {
            DeleteBuilder delete = client.delete();
            delete.inBackground((ignored, event) -> future.complete(event), future)
                    .forPath("/not-exist-path");
            return (BackgroundOperation<?>) delete;
        });
    }

    @Test
    public void testBackgroundExistsWithConcurrentClose() throws Exception {
        testBackgroundOperationWithConcurrentClose((client, future) -> {
            ExistsBuilder exists = client.checkExists();
            exists.inBackground((ignored, event) -> future.complete(event), future)
                    .forPath("/not-exist-path");
            return (BackgroundOperation<?>) exists;
        });
    }

    @Test
    public void testBackgroundGetDataWithConcurrentClose() throws Exception {
        testBackgroundOperationWithConcurrentClose((client, future) -> {
            GetDataBuilder getData = client.getData();
            getData.inBackground((ignored, event) -> future.complete(event), future)
                    .forPath("/not-exist-path");
            return (BackgroundOperation<?>) getData;
        });
    }

    @Test
    public void testBackgroundSetDataWithConcurrentClose() throws Exception {
        testBackgroundOperationWithConcurrentClose((client, future) -> {
            SetDataBuilder setData = client.setData();
            setData.inBackground((ignored, event) -> future.complete(event), future)
                    .forPath("/not-exist-path");
            return (BackgroundOperation<?>) setData;
        });
    }

    @Test
    public void testBackgroundChildrenWithConcurrentClose() throws Exception {
        testBackgroundOperationWithConcurrentClose((client, future) -> {
            GetChildrenBuilder children = client.getChildren();
            children.inBackground((ignored, event) -> future.complete(event), future)
                    .forPath("/not-exist-path");
            return (BackgroundOperation<?>) children;
        });
    }

    @Test
    public void testBackgroundGetACLWithConcurrentClose() throws Exception {
        testBackgroundOperationWithConcurrentClose((client, future) -> {
            GetACLBuilder getACL = client.getACL();
            getACL.inBackground((ignored, event) -> future.complete(event), future)
                    .forPath("/not-exist-path");
            return (BackgroundOperation<?>) getACL;
        });
    }

    @Test
    public void testBackgroundSetACLWithConcurrentClose() throws Exception {
        testBackgroundOperationWithConcurrentClose((client, future) -> {
            SetACLBuilder setACL = client.setACL();
            setACL.withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                    .inBackground((ignored, event) -> future.complete(event), future)
                    .forPath("/not-exist-path");
            return (BackgroundOperation<?>) setACL;
        });
    }

    @Test
    public void testBackgroundTransactionWithConcurrentClose() throws Exception {
        testBackgroundOperationWithConcurrentClose((client, future) -> {
            CuratorOp delete = client.transactionOp().delete().forPath("/not-exist-path");
            CuratorMultiTransaction transaction = client.transaction();
            transaction
                    .inBackground((ignored, event) -> future.complete(event), future)
                    .forOperations(delete);
            return (BackgroundOperation<?>) transaction;
        });
    }

    @Test
    public void testBackgroundRemoveWatchesWithConcurrentClose() throws Exception {
        testBackgroundOperationWithConcurrentClose((client, future) -> {
            RemoveWatchesBuilderImpl removeWatches =
                    (RemoveWatchesBuilderImpl) client.watches().removeAll();
            removeWatches
                    .inBackground((ignored, event) -> future.complete(event), future)
                    .forPath("/not-exist-path");
            return removeWatches;
        });
    }

    @Test
    public void testBackgroundGetDataWithWatch() throws Exception {
        final byte[] data1 = {1, 2, 3};
        final byte[] data2 = {4, 5, 6, 7};

        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try {
            final CountDownLatch watchedLatch = new CountDownLatch(1);
            client.getCuratorListenable().addListener(new CuratorListener() {
                @Override
                public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
                    if (event.getType() == CuratorEventType.GET_DATA) {
                        assertEquals(event.getPath(), "/test");
                        assertArrayEquals(event.getData(), data1);
                        ((CountDownLatch) event.getContext()).countDown();
                    } else if (event.getType() == CuratorEventType.WATCHED) {
                        if (event.getWatchedEvent().getType() == Watcher.Event.EventType.NodeDataChanged) {
                            assertEquals(event.getPath(), "/test");
                            watchedLatch.countDown();
                        }
                    }
                }
            });

            client.create().forPath("/test", data1);

            CountDownLatch backgroundLatch = new CountDownLatch(1);
            client.getData().watched().inBackground(backgroundLatch).forPath("/test");
            assertTrue(backgroundLatch.await(10, TimeUnit.SECONDS));

            client.setData().forPath("/test", data2);
            assertTrue(watchedLatch.await(10, TimeUnit.SECONDS));
            byte[] checkData = client.getData().forPath("/test");
            assertArrayEquals(checkData, data2);
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testBackgroundCreate() throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try {
            client.getCuratorListenable().addListener(new CuratorListener() {
                @Override
                public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
                    if (event.getType() == CuratorEventType.CREATE) {
                        assertEquals(event.getPath(), "/test");
                        ((CountDownLatch) event.getContext()).countDown();
                    }
                }
            });

            CountDownLatch latch = new CountDownLatch(1);
            client.create().inBackground(latch).forPath("/test", new byte[] {1, 2, 3});
            assertTrue(latch.await(10, TimeUnit.SECONDS));
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testBackgroundPathWithNamespace() throws Exception {
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        try (CuratorFramework client = builder.connectString(server.getConnectString())
                .retryPolicy(new RetryOneTime(1))
                .build()) {
            client.start();
            CuratorFramework namespaceZoo = client.usingNamespace("zoo");
            BlockingQueue<CuratorEvent> events = new LinkedBlockingQueue<>();
            BackgroundCallback callback = (CuratorFramework ignored, CuratorEvent event) -> {
                events.add(event);
            };

            namespaceZoo
                    .create()
                    .creatingParentsIfNeeded()
                    .inBackground(callback)
                    .forPath("/zoo/a");
            CuratorEvent event = events.poll(10, TimeUnit.SECONDS);
            assertNotNull(event);
            assertEquals("/zoo/a", event.getPath());
            assertEquals("/zoo/a", event.getName());

            client.checkExists().inBackground(callback).forPath("/zoo/zoo/a");
            event = events.poll(10, TimeUnit.SECONDS);
            assertNotNull(event);
            assertEquals("/zoo/zoo/a", event.getPath());
        }
    }

    @Test
    public void testBackgroundConfigPathWithNamespace() throws Exception {
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        try (CuratorFramework client = builder.connectString(server.getConnectString())
                .retryPolicy(new RetryOneTime(1))
                .authorization("digest", superUserPassword.getBytes())
                .build()) {
            client.start();
            assertBackgroundConfigPath(client);
            assertBackgroundConfigPath(client.usingNamespace("zoo"));
            assertBackgroundConfigPath(client.usingNamespace("zookeeper"));
            assertBackgroundConfigPath(client.usingNamespace("zookeeper/config"));
            assertBackgroundConfigPath(client.usingNamespace("foo"));
            assertBackgroundConfigPath(client.usingNamespace("foo/bar"));
        }
    }

    private void assertBackgroundConfigPath(CuratorFramework client) throws Exception {
        BlockingQueue<CuratorEvent> events = new LinkedBlockingQueue<>();
        BlockingQueue<WatchedEvent> watchedEvents = new LinkedBlockingQueue<>();
        BackgroundCallback callback = (CuratorFramework ignored, CuratorEvent event) -> {
            events.add(event);
        };
        Watcher watcher = watchedEvents::add;

        client.getConfig().usingWatcher(watcher).inBackground(callback).forEnsemble();
        CuratorEvent event = events.poll(10, TimeUnit.SECONDS);
        assertNotNull(event);
        assertEquals("/zookeeper/config", event.getPath());

        client.usingNamespace(null).setData().forPath("/zookeeper/config", event.getData());
        WatchedEvent watchedEvent = watchedEvents.poll(10, TimeUnit.SECONDS);
        assertNotNull(watchedEvent);
        assertEquals(Watcher.Event.EventType.NodeDataChanged, watchedEvent.getType());
        assertEquals("/zookeeper/config", watchedEvent.getPath());
    }

    @Test
    public void testCreateModes() throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try {
            byte[] writtenBytes = {1, 2, 3};
            client.create().forPath("/test", writtenBytes); // should be persistent

            client.close();
            client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
            client.start();

            byte[] readBytes = client.getData().forPath("/test");
            assertArrayEquals(writtenBytes, readBytes);

            client.create().withMode(CreateMode.EPHEMERAL).forPath("/ghost", writtenBytes);

            client.close();
            client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
            client.start();

            readBytes = client.getData().forPath("/test");
            assertArrayEquals(writtenBytes, readBytes);
            Stat stat = client.checkExists().forPath("/ghost");
            assertNull(stat);

            String realPath =
                    client.create().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath("/pseq", writtenBytes);
            assertNotSame(realPath, "/pseq");

            client.close();
            client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
            client.start();

            readBytes = client.getData().forPath(realPath);
            assertArrayEquals(writtenBytes, readBytes);

            realPath = client.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath("/eseq", writtenBytes);
            assertNotSame(realPath, "/eseq");

            client.close();
            client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
            client.start();

            stat = client.checkExists().forPath(realPath);
            assertNull(stat);
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testConfigurableZookeeper() throws Exception {
        CuratorFramework client = null;
        try {
            ZKClientConfig zkClientConfig = new ZKClientConfig();
            String zookeeperRequestTimeout = "30000";
            zkClientConfig.setProperty(ZKClientConfig.ZOOKEEPER_REQUEST_TIMEOUT, zookeeperRequestTimeout);
            client = CuratorFrameworkFactory.newClient(
                    server.getConnectString(), 30000, 30000, new RetryOneTime(1), zkClientConfig);
            client.start();

            byte[] writtenBytes = {1, 2, 3};
            client.create().forPath("/test", writtenBytes);

            byte[] readBytes = client.getData().forPath("/test");
            assertArrayEquals(writtenBytes, readBytes);
            assertEquals(
                    zookeeperRequestTimeout,
                    client.getZookeeperClient()
                            .getZooKeeper()
                            .getClientConfig()
                            .getProperty(ZKClientConfig.ZOOKEEPER_REQUEST_TIMEOUT));

        } catch (NoSuchMethodError e) {
            log.debug("NoSuchMethodError: ", e);
            log.info("Got NoSuchMethodError, meaning probably this cannot be used with ZooKeeper version < 3.6.1");
        } finally {
            try {
                CloseableUtils.closeQuietly(client);
            } catch (NoSuchMethodError e) {
                log.debug("close: NoSuchMethodError: ", e);
                log.info(
                        "close: Got NoSuchMethodError, meaning probably this cannot be used with ZooKeeper version < 3.6.1");
            }
        }
    }

    @Test
    public void testSimple() throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try {
            String path = client.create().withMode(CreateMode.PERSISTENT).forPath("/test", new byte[] {1, 2, 3});
            assertEquals(path, "/test");
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testSequentialWithTrailingSeparator() throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try {
            client.create().forPath("/test");
            // This should create a node in the form of "/test/00000001"
            String path =
                    client.create().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath("/test/");
            assertTrue(path.startsWith("/test/"));
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }
}
