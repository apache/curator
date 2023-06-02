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

package org.apache.curator.framework.recipes.nodes;

import static org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode.BUILD_INITIAL_CACHE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.Timing;
import org.apache.curator.test.compatibility.CuratorTestBase;
import org.apache.curator.test.compatibility.Timing2;
import org.apache.curator.utils.ZKPaths;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestPersistentTtlNode extends CuratorTestBase {
    private final Timing timing = new Timing();
    private final long ttlMs = timing.multiple(.10).milliseconds(); // a small number

    @BeforeAll
    public static void setUpClass() {
        System.setProperty("zookeeper.extendedTypesEnabled", "true");
    }

    @BeforeEach
    @Override
    public void setup() throws Exception {
        System.setProperty("znode.container.checkIntervalMs", "1");
        super.setup();
    }

    @AfterEach
    @Override
    public void teardown() throws Exception {
        System.clearProperty("znode.container.checkIntervalMs");
        super.teardown();
    }

    @Test
    public void testBasic() throws Exception {
        try (CuratorFramework client =
                CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1))) {
            client.start();

            try (PersistentTtlNode node = new PersistentTtlNode(client, "/test", ttlMs, new byte[0])) {
                node.start();
                assertTrue(node.waitForInitialCreate(timing.session(), TimeUnit.MILLISECONDS));

                for (int i = 0; i < 5; ++i) {
                    Thread.sleep(ttlMs + (ttlMs / 2)); // sleep a bit more than the TTL
                    assertNotNull(client.checkExists().forPath("/test"));
                }
            }
            assertNotNull(client.checkExists().forPath("/test"));

            timing.sleepABit();
            assertNull(client.checkExists().forPath("/test"));
        }
    }

    @Test
    public void testForcedDeleteOfTouchNode() throws Exception {
        try (CuratorFramework client =
                CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1))) {
            client.start();

            try (PersistentTtlNode node = new PersistentTtlNode(client, "/test", ttlMs, new byte[0])) {
                node.start();
                assertTrue(node.waitForInitialCreate(timing.session(), TimeUnit.MILLISECONDS));

                for (int i = 0; i < 5; ++i) {
                    Thread.sleep(ttlMs);
                    client.delete()
                            .quietly()
                            .forPath(ZKPaths.makePath("test", PersistentTtlNode.DEFAULT_CHILD_NODE_NAME));
                }

                timing.sleepABit();
                assertNotNull(client.checkExists().forPath("/test"));
            }
        }
    }

    @Test
    public void testRecreationOfParentNodeWithParentCreationOff() throws Exception {
        final byte[] TEST_DATA = "hey".getBytes();
        Timing2 timing = new Timing2();
        String containerPath = ZKPaths.makePath("test", "one", "two");
        String childPath = ZKPaths.makePath("test", "one", "two", PersistentTtlNode.DEFAULT_CHILD_NODE_NAME);

        try (CuratorFramework client = CuratorFrameworkFactory.newClient(
                server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1))) {
            client.start();
            client.create().creatingParentsIfNeeded().forPath("/test/one");

            try (PersistentTtlNode node = new PersistentTtlNode(client, containerPath, ttlMs, TEST_DATA, false)) {
                node.start();
                assertTrue(node.waitForInitialCreate(timing.milliseconds(), TimeUnit.MILLISECONDS));

                Thread.sleep(ttlMs + (ttlMs / 2));
                assertNotNull(client.checkExists().forPath(containerPath));
                assertNotNull(client.checkExists().forPath(childPath));

                client.delete().deletingChildrenIfNeeded().forPath("/test/one");
                timing.sleepABit();

                // The underlying persistent node should not be able to recreate itself as the lazy parent creation is
                // disabled
                assertNull(client.checkExists().forPath(containerPath));
                assertNull(client.checkExists().forPath(childPath));

                assertThrows(IllegalStateException.class, () -> node.setData(new byte[0]));

                // The underlying persistent node data should still be the initial one
                assertArrayEquals(TEST_DATA, node.getData());
            }
        }
    }

    @Test
    public void testEventsOnParent() throws Exception {
        try (CuratorFramework client =
                CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1))) {
            client.start();

            try (PersistentTtlNode node = new PersistentTtlNode(client, "/test", ttlMs, new byte[0])) {
                try (PathChildrenCache cache = new PathChildrenCache(client, "/", true)) {
                    final Semaphore changes = new Semaphore(0);
                    PathChildrenCacheListener listener = new PathChildrenCacheListener() {
                        @Override
                        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                            if ((event.getType() == PathChildrenCacheEvent.Type.CHILD_UPDATED)
                                    && "/test".equals(event.getData().getPath())) {
                                changes.release();
                            }
                        }
                    };
                    cache.getListenable().addListener(listener);

                    node.start();
                    assertTrue(node.waitForInitialCreate(timing.session(), TimeUnit.MILLISECONDS));
                    cache.start(BUILD_INITIAL_CACHE);

                    assertEquals(changes.availablePermits(), 0);
                    timing.sleepABit();
                    assertEquals(changes.availablePermits(), 0);

                    client.setData().forPath("/test", "changed".getBytes());
                    assertTrue(timing.acquireSemaphore(changes));
                    timing.sleepABit();
                    assertEquals(changes.availablePermits(), 0);
                }
            }

            timing.sleepABit();

            assertNull(client.checkExists().forPath("/test"));
        }
    }
}
