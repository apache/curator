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

package org.apache.curator.framework.recipes.cache;

import static org.apache.curator.framework.recipes.cache.CuratorCache.Options.DO_NOT_CLEAR_ON_CLOSE;
import static org.apache.curator.framework.recipes.cache.CuratorCacheListener.builder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.compatibility.CuratorTestBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(CuratorTestBase.zk36Group)
public class TestCuratorCache extends CuratorTestBase {
    @Test
    public void testUpdateWhenNotCachingData() throws Exception // mostly copied from TestPathChildrenCache
            {
        CuratorCacheStorage storage = new StandardCuratorCacheStorage(false);
        try (CuratorFramework client = CuratorFrameworkFactory.newClient(
                server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1))) {
            client.start();
            final CountDownLatch updatedLatch = new CountDownLatch(1);
            final CountDownLatch addedLatch = new CountDownLatch(1);
            client.create().creatingParentsIfNeeded().forPath("/test");
            try (CuratorCache cache =
                    CuratorCache.builder(client, "/test").withStorage(storage).build()) {
                cache.listenable()
                        .addListener(builder()
                                .forChanges((__, ___) -> updatedLatch.countDown())
                                .build());
                cache.listenable()
                        .addListener(builder()
                                .forCreates(__ -> addedLatch.countDown())
                                .build());
                cache.start();

                client.create().forPath("/test/foo", "first".getBytes());
                assertTrue(timing.awaitLatch(addedLatch));

                client.setData().forPath("/test/foo", "something new".getBytes());
                assertTrue(timing.awaitLatch(updatedLatch));
            }
        }
    }

    @Test
    public void testAfterInitialized() throws Exception {
        try (CuratorFramework client = CuratorFrameworkFactory.newClient(
                server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1))) {
            client.start();
            client.create().creatingParentsIfNeeded().forPath("/test");
            client.create().creatingParentsIfNeeded().forPath("/test/one");
            client.create().creatingParentsIfNeeded().forPath("/test/one/two");
            client.create().creatingParentsIfNeeded().forPath("/test/one/two/three");
            try (CuratorCache cache = CuratorCache.build(client, "/test")) {
                CountDownLatch initializedLatch = new CountDownLatch(1);
                AtomicInteger eventCount = new AtomicInteger(0);
                CuratorCacheListener listener = new CuratorCacheListener() {
                    @Override
                    public void event(Type type, ChildData oldData, ChildData data) {
                        eventCount.incrementAndGet();
                    }

                    @Override
                    public void initialized() {
                        initializedLatch.countDown();
                    }
                };
                cache.listenable()
                        .addListener(
                                builder().forAll(listener).afterInitialized().build());
                cache.start();
                assertTrue(timing.awaitLatch(initializedLatch));

                assertEquals(initializedLatch.getCount(), 0);
                assertEquals(cache.size(), 4);
                assertTrue(cache.get("/test").isPresent());
                assertTrue(cache.get("/test/one").isPresent());
                assertTrue(cache.get("/test/one/two").isPresent());
                assertTrue(cache.get("/test/one/two/three").isPresent());
            }
        }
    }

    @Test
    public void testListenerBuilder() throws Exception {
        try (CuratorFramework client = CuratorFrameworkFactory.newClient(
                server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1))) {
            client.start();
            try (CuratorCache cache = CuratorCache.build(client, "/test")) {
                Semaphore all = new Semaphore(0);
                Semaphore deletes = new Semaphore(0);
                Semaphore changes = new Semaphore(0);
                Semaphore creates = new Semaphore(0);
                Semaphore createsAndChanges = new Semaphore(0);

                CuratorCacheListener listener = builder()
                        .forAll((__, ___, ____) -> all.release())
                        .forDeletes(__ -> deletes.release())
                        .forChanges((__, ___) -> changes.release())
                        .forCreates(__ -> creates.release())
                        .forCreatesAndChanges((__, ___) -> createsAndChanges.release())
                        .build();
                cache.listenable().addListener(listener);
                cache.start();

                client.create().forPath("/test");
                assertTrue(timing.acquireSemaphore(all, 1));
                assertTrue(timing.acquireSemaphore(creates, 1));
                assertTrue(timing.acquireSemaphore(createsAndChanges, 1));
                assertEquals(changes.availablePermits(), 0);
                assertEquals(deletes.availablePermits(), 0);

                client.setData().forPath("/test", "new".getBytes());
                assertTrue(timing.acquireSemaphore(all, 1));
                assertTrue(timing.acquireSemaphore(changes, 1));
                assertTrue(timing.acquireSemaphore(createsAndChanges, 1));
                assertEquals(creates.availablePermits(), 0);
                assertEquals(deletes.availablePermits(), 0);

                client.delete().forPath("/test");
                assertTrue(timing.acquireSemaphore(all, 1));
                assertTrue(timing.acquireSemaphore(deletes, 1));
                assertEquals(creates.availablePermits(), 0);
                assertEquals(changes.availablePermits(), 0);
                assertEquals(createsAndChanges.availablePermits(), 0);
            }
        }
    }

    @Test
    public void testClearOnClose() throws Exception {
        try (CuratorFramework client = CuratorFrameworkFactory.newClient(
                server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1))) {
            CuratorCacheStorage storage;
            client.start();

            try (CuratorCache cache = CuratorCache.builder(client, "/test")
                    .withOptions(DO_NOT_CLEAR_ON_CLOSE)
                    .build()) {
                cache.start();
                storage = ((CuratorCacheImpl) cache).storage();

                client.create().forPath("/test", "foo".getBytes());
                client.create().forPath("/test/bar", "bar".getBytes());
                timing.sleepABit();
            }
            assertEquals(storage.size(), 2);

            try (CuratorCache cache = CuratorCache.build(client, "/test")) {
                cache.start();
                storage = ((CuratorCacheImpl) cache).storage();

                timing.sleepABit();
            }
            assertEquals(storage.size(), 0);
        }
    }

    // CURATOR-690 - CuratorCache fails to load the cache if there are more than 64K child ZNodes
    @Test
    public void testGreaterThan64kZNodes() throws Exception {
        final CuratorCacheStorage storage = new StandardCuratorCacheStorage(false);

        // Phaser has a hard-limit of 64k registrants; we need to create more than that to trigger the initial problem.
        final int zNodeCount = 0xffff + 5;

        try (CuratorFramework client = CuratorFrameworkFactory.newClient(
                server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1))) {
            client.start();
            final CountDownLatch initializedLatch = new CountDownLatch(1);
            client.create().creatingParentsIfNeeded().forPath("/test");


            for (int i = 0; i < zNodeCount; i++) {
                client.create().forPath("/test/node_" + i);
            }

            try (CuratorCache cache = CuratorCache.builder(client, "/test").withStorage(storage).build()) {
                cache.listenable()
                        .addListener(builder()
                                .forInitialized(() -> initializedLatch.countDown())
                                .build());
                cache.start();

                assertTrue(timing.awaitLatch(initializedLatch));
                assertEquals(zNodeCount + 1, cache.size(), "Cache size should be equal to the number of zNodes created plus the root");
            }
        }
    }
}
