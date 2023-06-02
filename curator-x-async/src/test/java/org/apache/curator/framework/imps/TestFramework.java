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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.AuthInfo;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.AsyncStage;
import org.apache.curator.x.async.api.CreateOption;
import org.apache.curator.x.async.api.DeleteOption;
import org.apache.curator.x.async.api.ExistsOption;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("deprecation")
public class TestFramework extends BaseClassForTests {
    @BeforeEach
    @Override
    public void setup() throws Exception {
        System.setProperty("znode.container.checkIntervalMs", "1000");
        super.setup();
    }

    @AfterEach
    @Override
    public void teardown() throws Exception {
        System.clearProperty("znode.container.checkIntervalMs");
        super.teardown();
    }

    @Test
    public void testQuietDelete() throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try {
            client.start();
            AsyncCuratorFramework async = AsyncCuratorFramework.wrap(client);

            async.delete().withOptions(EnumSet.of(DeleteOption.quietly)).forPath("/foo/bar");

            final BlockingQueue<Integer> rc = new LinkedBlockingQueue<>();
            BackgroundCallback backgroundCallback = (client1, event) -> rc.add(event.getResultCode());
            async.delete()
                    .withOptions(EnumSet.of(DeleteOption.quietly))
                    .forPath("/foo/bar/hey")
                    .handle((v, e) -> {
                        if (e == null) {
                            rc.add(KeeperException.Code.OK.intValue());
                        } else {
                            rc.add(((KeeperException) e).code().intValue());
                        }
                        return null;
                    });

            Integer code = rc.poll(new Timing().milliseconds(), TimeUnit.MILLISECONDS);
            assertNotNull(code);
            assertEquals(code.intValue(), KeeperException.Code.OK.intValue());
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
            AsyncCuratorFramework async = AsyncCuratorFramework.wrap(client);
            BlockingQueue<String> queue = new LinkedBlockingQueue<String>();
            async.create()
                    .forPath("/base")
                    .thenRun(() -> async.watched()
                            .getChildren()
                            .forPath("/base")
                            .event()
                            .handle((event, x) -> {
                                try {
                                    queue.put(event.getPath());
                                } catch (InterruptedException e) {
                                    throw new Error(e);
                                }
                                return null;
                            }))
                    .thenRun(() -> async.create().forPath("/base/child"));

            String path = queue.take();
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
                AsyncCuratorFramework async = AsyncCuratorFramework.wrap(client);
                async.setData()
                        .forPath("/test", "test".getBytes())
                        .toCompletableFuture()
                        .get();
            } catch (ExecutionException e) {
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
                AsyncCuratorFramework async = AsyncCuratorFramework.wrap(client);
                async.setData()
                        .forPath("/test", "test".getBytes())
                        .toCompletableFuture()
                        .get();
                fail("Should have failed with auth exception");
            } catch (ExecutionException e) {
                // expected
            }
        } finally {
            CloseableUtils.closeQuietly(client);
        }
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
                AsyncCuratorFramework async = AsyncCuratorFramework.wrap(client);
                async.setData()
                        .forPath("/test", "test".getBytes())
                        .toCompletableFuture()
                        .get();
            } catch (ExecutionException e) {
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
                AsyncCuratorFramework async = AsyncCuratorFramework.wrap(client);
                async.setData()
                        .forPath("/test", "test".getBytes())
                        .toCompletableFuture()
                        .get();
            } catch (ExecutionException e) {
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
                AsyncCuratorFramework async = AsyncCuratorFramework.wrap(client);
                async.setData()
                        .forPath("/test", "test".getBytes())
                        .toCompletableFuture()
                        .get();
                fail("Should have failed with auth exception");
            } catch (ExecutionException e) {
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
                .retryPolicy(new ExponentialBackoffRetry(100, 5))
                .build();
        client.start();
        try {
            final CountDownLatch lostLatch = new CountDownLatch(1);
            ConnectionStateListener listener = (client1, newState) -> {
                if (newState == ConnectionState.LOST) {
                    lostLatch.countDown();
                }
            };
            client.getConnectionStateListenable().addListener(listener);

            ACL acl = new ACL(ZooDefs.Perms.WRITE, ZooDefs.Ids.AUTH_IDS);
            List<ACL> aclList = Lists.newArrayList(acl);
            client.create().withACL(aclList).forPath("/test", "test".getBytes());

            server.stop();
            assertTrue(timing.awaitLatch(lostLatch));
            try {
                AsyncCuratorFramework async = AsyncCuratorFramework.wrap(client);
                async.checkExists().forPath("/").toCompletableFuture().get();
                fail("Connection should be down");
            } catch (ExecutionException e) {
                // expected
            }

            server.restart();
            try {
                AsyncCuratorFramework async = AsyncCuratorFramework.wrap(client);
                async.setData()
                        .forPath("/test", "test".getBytes())
                        .toCompletableFuture()
                        .get();
            } catch (ExecutionException e) {
                fail("Auth failed", e);
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
            AsyncCuratorFramework async = AsyncCuratorFramework.wrap(client);
            async.create()
                    .withOptions(EnumSet.of(CreateOption.createParentsIfNeeded))
                    .forPath("/one/two/three", "foo".getBytes())
                    .toCompletableFuture()
                    .get();
            byte[] data = async.getData()
                    .forPath("/one/two/three")
                    .toCompletableFuture()
                    .get();
            assertArrayEquals(data, "foo".getBytes());

            async.create()
                    .withOptions(EnumSet.of(CreateOption.createParentsIfNeeded))
                    .forPath("/one/two/another", "bar".getBytes());
            data = async.getData()
                    .forPath("/one/two/another")
                    .toCompletableFuture()
                    .get();
            assertArrayEquals(data, "bar".getBytes());
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
            AsyncCuratorFramework async = AsyncCuratorFramework.wrap(client);
            async.create()
                    .withOptions(EnumSet.of(CreateOption.createParentsAsContainers))
                    .forPath("/one/two/three", "foo".getBytes())
                    .toCompletableFuture()
                    .get();
            byte[] data = async.getData()
                    .forPath("/one/two/three")
                    .toCompletableFuture()
                    .get();
            assertArrayEquals(data, "foo".getBytes());

            async.delete().forPath("/one/two/three").toCompletableFuture().get();
            new Timing().sleepABit();

            assertNull(async.checkExists()
                    .forPath("/one/two")
                    .toCompletableFuture()
                    .get());
            new Timing().sleepABit();
            assertNull(async.checkExists().forPath("/one").toCompletableFuture().get());
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testCreateWithProtection() throws ExecutionException, InterruptedException {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try {
            client.start();
            AsyncCuratorFramework async = AsyncCuratorFramework.wrap(client);
            String path = async.create()
                    .withOptions(Collections.singleton(CreateOption.doProtected))
                    .forPath("/yo")
                    .toCompletableFuture()
                    .get();
            String node = ZKPaths.getNodeFromPath(path);
            // CURATOR-489: confirm that the node contains a valid UUID, eg '_c_53345f98-9423-4e0c-a7b5-9f819e3ec2e1-yo'
            assertTrue(ProtectedUtils.isProtectedZNode(node));
            assertEquals(ProtectedUtils.normalize(node), "yo");
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
            AsyncCuratorFramework async = AsyncCuratorFramework.wrap(client);

            assertNull(client.checkExists().forPath("/one/two"));
            async.create()
                    .withOptions(EnumSet.of(CreateOption.createParentsAsContainers))
                    .forPath("/one/two/three")
                    .toCompletableFuture()
                    .get();
            assertNotNull(async.checkExists()
                    .forPath("/one/two")
                    .toCompletableFuture()
                    .get());

            async.delete()
                    .withOptions(EnumSet.of(DeleteOption.deletingChildrenIfNeeded))
                    .forPath("/one")
                    .toCompletableFuture()
                    .get();
            assertNull(client.checkExists().forPath("/one"));

            assertNull(async.checkExists()
                    .forPath("/one/two")
                    .toCompletableFuture()
                    .get());
            async.checkExists()
                    .withOptions(EnumSet.of(ExistsOption.createParentsAsContainers))
                    .forPath("/one/two/three")
                    .toCompletableFuture()
                    .get();
            assertNotNull(async.checkExists()
                    .forPath("/one/two")
                    .toCompletableFuture()
                    .get());
            assertNull(async.checkExists()
                    .forPath("/one/two/three")
                    .toCompletableFuture()
                    .get());
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testExistsCreatingParents() throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try {
            client.start();
            AsyncCuratorFramework async = AsyncCuratorFramework.wrap(client);

            assertNull(async.checkExists()
                    .forPath("/one/two")
                    .toCompletableFuture()
                    .get());
            async.checkExists()
                    .withOptions(EnumSet.of(ExistsOption.createParentsAsContainers))
                    .forPath("/one/two/three")
                    .toCompletableFuture()
                    .get();
            assertNotNull(async.checkExists()
                    .forPath("/one/two")
                    .toCompletableFuture()
                    .get());
            assertNull(async.checkExists()
                    .forPath("/one/two/three")
                    .toCompletableFuture()
                    .get());
            assertNull(async.checkExists()
                    .withOptions(EnumSet.of(ExistsOption.createParentsAsContainers))
                    .forPath("/one/two/three")
                    .toCompletableFuture()
                    .get());
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
            AsyncCuratorFramework async = AsyncCuratorFramework.wrap(client);
            async.sync().forPath("/head").handle((v, e) -> {
                assertNull(v);
                assertNull(e);
                latch.countDown();
                return null;
            });
            assertTrue(latch.await(10, TimeUnit.SECONDS));
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testBackgroundDelete() throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try {
            AsyncCuratorFramework async = AsyncCuratorFramework.wrap(client);
            CountDownLatch latch = new CountDownLatch(1);
            async.create()
                    .forPath("/head")
                    .thenRun(() -> async.delete().forPath("/head").handle((v, e) -> {
                        assertNull(v);
                        assertNull(e);
                        latch.countDown();
                        return null;
                    }));
            assertTrue(latch.await(10, TimeUnit.SECONDS));
            assertNull(client.checkExists().forPath("/head"));
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testBackgroundDeleteWithChildren() throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try {
            client.getCuratorListenable().addListener((client1, event) -> {
                if (event.getType() == CuratorEventType.DELETE) {
                    assertEquals(event.getPath(), "/one/two");
                    ((CountDownLatch) event.getContext()).countDown();
                }
            });

            CountDownLatch latch = new CountDownLatch(1);
            AsyncCuratorFramework async = AsyncCuratorFramework.wrap(client);
            async.create()
                    .withOptions(EnumSet.of(CreateOption.createParentsIfNeeded))
                    .forPath("/one/two/three/four")
                    .thenRun(() -> async.delete()
                            .withOptions(EnumSet.of(DeleteOption.deletingChildrenIfNeeded))
                            .forPath("/one/two")
                            .handle((v, e) -> {
                                assertNull(v);
                                assertNull(e);
                                latch.countDown();
                                return null;
                            }));
            assertTrue(latch.await(10, TimeUnit.SECONDS));
            assertNull(client.checkExists().forPath("/one/two"));
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testDeleteGuaranteedWithChildren() throws Exception {
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        CuratorFramework client = builder.connectString(server.getConnectString())
                .retryPolicy(new RetryOneTime(1))
                .build();
        client.start();
        try {
            AsyncCuratorFramework async = AsyncCuratorFramework.wrap(client);
            async.create()
                    .withOptions(EnumSet.of(CreateOption.createParentsIfNeeded))
                    .forPath("/one/two/three/four/five/six", "foo".getBytes())
                    .toCompletableFuture()
                    .get();
            async.delete()
                    .withOptions(EnumSet.of(DeleteOption.guaranteed, DeleteOption.deletingChildrenIfNeeded))
                    .forPath("/one/two/three/four/five")
                    .toCompletableFuture()
                    .get();
            assertNull(async.checkExists()
                    .forPath("/one/two/three/four/five")
                    .toCompletableFuture()
                    .get());
            async.delete()
                    .withOptions(EnumSet.of(DeleteOption.guaranteed, DeleteOption.deletingChildrenIfNeeded))
                    .forPath("/one/two")
                    .toCompletableFuture()
                    .get();
            assertNull(async.checkExists()
                    .forPath("/one/two")
                    .toCompletableFuture()
                    .get());
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testGetSequentialChildren() throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try {
            Semaphore semaphore = new Semaphore(0);
            AsyncCuratorFramework async = AsyncCuratorFramework.wrap(client);
            async.create().forPath("/head").thenRun(() -> {
                for (int i = 0; i < 10; ++i) {
                    async.create()
                            .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                            .forPath("/head/child")
                            .thenRun(semaphore::release);
                }
            });

            assertTrue(new Timing().acquireSemaphore(semaphore, 10));
            List<String> children =
                    async.getChildren().forPath("/head").toCompletableFuture().get();
            assertEquals(children.size(), 10);
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testBackgroundGetDataWithWatch() throws Exception {
        final byte[] data1 = {1, 2, 3};
        final byte[] data2 = {4, 5, 6, 7};

        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try {
            AsyncCuratorFramework async = AsyncCuratorFramework.wrap(client);
            async.create().forPath("/test", data1).toCompletableFuture().get();

            CountDownLatch watchedLatch = new CountDownLatch(1);
            CountDownLatch backgroundLatch = new CountDownLatch(1);
            AsyncStage<byte[]> stage = async.watched().getData().forPath("/test");
            stage.event().handle((event, x) -> {
                assertEquals(event.getPath(), "/test");
                watchedLatch.countDown();
                return null;
            });
            stage.handle((d, x) -> {
                assertArrayEquals(d, data1);
                backgroundLatch.countDown();
                return null;
            });

            assertTrue(backgroundLatch.await(10, TimeUnit.SECONDS));

            async.setData().forPath("/test", data2);
            assertTrue(watchedLatch.await(10, TimeUnit.SECONDS));
            byte[] checkData =
                    async.getData().forPath("/test").toCompletableFuture().get();
            assertArrayEquals(checkData, data2);
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }
}
