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

package org.apache.curator.framework.recipes.locks;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.TestCleanState;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.Test;

public class TestInterProcessMultiLockBuilder extends BaseClassForTests {
    private static final String PARENT = "/locks/coll";
    private static final String CHILD = "/locks/coll/shard";

    private CuratorFramework newClient() {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        return client;
    }

    /** read lock on the parent, write lock on the child (Solr shard-level write) */
    private static InterProcessMultiLock childWrite(CuratorFramework client) {
        return InterProcessMultiLock.builder(client).read(PARENT).write(CHILD).build();
    }

    /** write lock on the parent (Solr collection-level write) */
    private static InterProcessMultiLock parentWrite(CuratorFramework client) {
        return InterProcessMultiLock.builder(client).write(PARENT).build();
    }

    @Test
    public void testEmptyLock() throws Exception {
        CuratorFramework client = newClient();
        try {
            InterProcessMultiLock lock = InterProcessMultiLock.builder(client).build();
            assertTrue(lock.acquire(1, TimeUnit.SECONDS));
            assertEquals(0, lock.size());
            assertTrue(lock.getLockNodePaths().isEmpty());
            lock.release();
        } finally {
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
    public void testMixedModes() throws Exception {
        CuratorFramework client1 = newClient();
        CuratorFramework client2 = newClient();
        try {
            InterProcessMultiLock lock = childWrite(client1);
            lock.acquire();
            assertTrue(lock.isAcquiredInThisProcess());

            // another write on the same child is blocked
            assertFalse(childWrite(client2).acquire(1, TimeUnit.SECONDS));
            // a write on the parent is blocked by our parent read lock
            assertFalse(parentWrite(client2).acquire(1, TimeUnit.SECONDS));
            // a write on a sibling child is allowed: parent reads are shared
            InterProcessMultiLock sibling = InterProcessMultiLock.builder(client2)
                    .read(PARENT)
                    .write(PARENT + "/other")
                    .build();
            assertTrue(sibling.acquire(5, TimeUnit.SECONDS));
            sibling.release();

            lock.release();
            assertFalse(lock.isAcquiredInThisProcess());
            InterProcessMultiLock parent = parentWrite(client2);
            assertTrue(parent.acquire(5, TimeUnit.SECONDS));
            parent.release();
        } finally {
            CloseableUtils.closeQuietly(client2);
            TestCleanState.closeAndTestClean(client1);
        }
    }

    @Test
    public void testLockData() throws Exception {
        CuratorFramework client = newClient();
        try {
            byte[] data = "payload".getBytes(StandardCharsets.UTF_8);
            InterProcessMultiLock lock = InterProcessMultiLock.builder(client)
                    .lockData(data)
                    .read(PARENT)
                    .write(CHILD)
                    .lockData(new byte[0])
                    .write(CHILD + "/empty")
                    .lockData(null)
                    .write(CHILD + "/default")
                    .build();
            lock.acquire();
            List<String> paths = lock.getLockNodePaths();

            // lockData applies to the locks added after it
            assertArrayEquals(data, client.getData().forPath(paths.get(0)));
            assertArrayEquals(data, client.getData().forPath(paths.get(1)));
            assertArrayEquals(new byte[0], client.getData().forPath(paths.get(2)));
            // null falls back to Curator's default payload (the local address)
            byte[] defaultData = client.getData().forPath(paths.get(3));
            assertTrue(defaultData.length > 0);
            assertFalse(Arrays.equals(data, defaultData));

            lock.release();
        } finally {
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
    public void testLockNodePathOfSingleLocks() throws Exception {
        CuratorFramework client = newClient();
        try {
            InterProcessMutex mutex = new InterProcessMutex(client, PARENT + "/mutex");
            assertNull(mutex.getLockNodePath());
            mutex.acquire();
            String mutexNode = mutex.getLockNodePath();
            assertTrue(mutexNode.startsWith(PARENT + "/mutex/"));
            assertNotNull(client.checkExists().forPath(mutexNode));
            mutex.release();
            assertNull(mutex.getLockNodePath());

            InterProcessSemaphoreMutex semaphoreMutex = new InterProcessSemaphoreMutex(client, PARENT + "/sem");
            assertNull(semaphoreMutex.getLockNodePath());
            semaphoreMutex.acquire();
            String semaphoreNode = semaphoreMutex.getLockNodePath();
            assertTrue(semaphoreNode.startsWith(PARENT + "/sem/leases/"));
            assertNotNull(client.checkExists().forPath(semaphoreNode));
            semaphoreMutex.release();
            assertNull(semaphoreMutex.getLockNodePath());

            // the classic multi lock of mutexes exposes its nodes too
            InterProcessMultiLock multi =
                    new InterProcessMultiLock(client, Arrays.asList(PARENT + "/m1", PARENT + "/m2"));
            assertEquals(Arrays.asList(null, null), multi.getLockNodePaths());
            multi.acquire();
            List<String> paths = multi.getLockNodePaths();
            assertTrue(paths.get(0).startsWith(PARENT + "/m1/"));
            assertTrue(paths.get(1).startsWith(PARENT + "/m2/"));
            assertEquals(2, multi.size());
            multi.release();

            // a lock type without a node falls back to the default
            InterProcessLock custom = new InterProcessLock() {
                @Override
                public void acquire() {}

                @Override
                public boolean acquire(long time, TimeUnit unit) {
                    return true;
                }

                @Override
                public void release() {}

                @Override
                public boolean isAcquiredInThisProcess() {
                    return true;
                }
            };
            assertNull(custom.getLockNodePath());
        } finally {
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
    public void testCustomLocks() throws Exception {
        CuratorFramework client1 = newClient();
        CuratorFramework client2 = newClient();
        try {
            InterProcessMutex mutex = new InterProcessMutex(client1, PARENT + "/mutex");
            InterProcessMultiLock lock = InterProcessMultiLock.builder(client1)
                    .read(PARENT)
                    .lock(mutex)
                    .build();
            lock.acquire();
            assertTrue(mutex.isOwnedByCurrentThread());
            assertEquals(mutex.getLockNodePath(), lock.getLockNodePaths().get(1));

            // the custom lock competes like any other
            assertFalse(new InterProcessMutex(client2, PARENT + "/mutex").acquire(1, TimeUnit.SECONDS));

            lock.release();
            assertFalse(mutex.isAcquiredInThisProcess());
            assertThrows(NullPointerException.class, () -> InterProcessMultiLock.builder(client1)
                    .lock(null));
        } finally {
            CloseableUtils.closeQuietly(client2);
            TestCleanState.closeAndTestClean(client1);
        }
    }

    @Test
    public void testLockNodePaths() throws Exception {
        CuratorFramework client = newClient();
        try {
            InterProcessMultiLock lock = childWrite(client);
            assertEquals(2, lock.size());
            assertNull(lock.getLockNodePaths().get(0));

            lock.acquire();
            List<String> paths = lock.getLockNodePaths();
            assertEquals(2, paths.size());
            assertTrue(paths.get(0).startsWith(PARENT + "/"));
            assertTrue(paths.get(0).contains(InterProcessReadWriteLock.READ_LOCK_NAME));
            assertTrue(paths.get(1).startsWith(CHILD + "/"));
            assertTrue(paths.get(1).contains(InterProcessReadWriteLock.WRITE_LOCK_NAME));
            for (String path : paths) {
                assertNotNull(client.checkExists().forPath(path));
            }
            lock.release();
        } finally {
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
    public void testMirroring() throws Exception {
        CuratorFramework parentClient = newClient();
        CuratorFramework childClient = newClient();
        try {
            // parent task holds a collection-level write lock
            InterProcessMultiLock parentLock = parentWrite(parentClient);
            parentLock.acquire();
            String parentNode = parentLock.getLockNodePaths().get(0);

            // a sub-task mirrors it and takes a shard write lock of its own, from another client
            InterProcessMultiLock childLock = InterProcessMultiLock.builder(childClient)
                    .mirror(PARENT, parentNode, false)
                    .write(CHILD)
                    .build();
            assertTrue(childLock.acquire(5, TimeUnit.SECONDS));
            List<String> childPaths = childLock.getLockNodePaths();
            assertEquals(parentNode, childPaths.get(0));
            assertTrue(childPaths.get(1).startsWith(CHILD + "/"));

            // a grandchild can mirror both
            InterProcessMultiLock grandchild = InterProcessMultiLock.builder(childClient)
                    .mirror(PARENT, childPaths.get(0), false)
                    .mirror(CHILD, childPaths.get(1), true)
                    .build();
            assertTrue(grandchild.acquire(5, TimeUnit.SECONDS));
            grandchild.release();
            assertFalse(grandchild.isAcquiredInThisProcess());

            // releasing the mirror must not release the parent's node
            childLock.release();
            assertNotNull(parentClient.checkExists().forPath(parentNode));
            assertTrue(parentLock.isAcquiredInThisProcess());

            // and others are still excluded by the parent's write lock
            assertFalse(childWrite(childClient).acquire(1, TimeUnit.SECONDS));

            parentLock.release();
        } finally {
            CloseableUtils.closeQuietly(childClient);
            TestCleanState.closeAndTestClean(parentClient);
        }
    }

    @Test
    public void testMirrorOfMissingNodeFailsAndRollsBack() throws Exception {
        CuratorFramework client1 = newClient();
        CuratorFramework client2 = newClient();
        try {
            InterProcessMultiLock holder = parentWrite(client1);
            holder.acquire();
            String node = holder.getLockNodePaths().get(0);
            holder.release();

            InterProcessMultiLock lock = InterProcessMultiLock.builder(client2)
                    .write(CHILD + "/first")
                    .mirror(PARENT, node, true)
                    .build();
            assertThrows(KeeperException.NoNodeException.class, lock::acquire);
            assertFalse(lock.isAcquiredInThisProcess());

            // the owned lock acquired before the failing mirror was released
            InterProcessReadWriteLock other = new InterProcessReadWriteLock(client1, CHILD + "/first");
            assertTrue(other.writeLock().acquire(5, TimeUnit.SECONDS));
            other.writeLock().release();
        } finally {
            CloseableUtils.closeQuietly(client2);
            TestCleanState.closeAndTestClean(client1);
        }
    }

    @Test
    public void testMirrorValidation() throws Exception {
        CuratorFramework client = newClient();
        try {
            InterProcessMultiLock.Builder builder = InterProcessMultiLock.builder(client);
            String readNode = PARENT + "/_c_abc-" + InterProcessReadWriteLock.READ_LOCK_NAME + "0000000001";
            String writeNode = PARENT + "/_c_abc-" + InterProcessReadWriteLock.WRITE_LOCK_NAME + "0000000001";

            // write cannot mirror a read node
            assertThrows(IllegalArgumentException.class, () -> builder.mirror(PARENT, readNode, true));
            // node must be directly under the base path
            assertThrows(IllegalArgumentException.class, () -> builder.mirror(CHILD, writeNode, false));
            // not a lock node
            assertThrows(IllegalArgumentException.class, () -> builder.mirror(PARENT, PARENT + "/foo", false));
            // valid
            builder.mirror(PARENT, readNode, false);
            builder.mirror(PARENT, writeNode, false);
            builder.mirror(PARENT, writeNode, true);
            assertEquals(3, builder.build().size());
        } finally {
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
    public void testReleaseWithoutAcquireThrows() throws Exception {
        CuratorFramework client = newClient();
        try {
            // a single lock surfaces the underlying exception directly
            InterProcessMultiLock owned =
                    InterProcessMultiLock.builder(client).write(PARENT).build();
            assertThrows(IllegalMonitorStateException.class, owned::release);
            // multiple failures are chained by InterProcessMultiLock
            assertThrows(Exception.class, childWrite(client)::release);
        } finally {
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
    public void testReentrancyAndSeparateInstances() throws Exception {
        CuratorFramework client1 = newClient();
        CuratorFramework client2 = newClient();
        try {
            InterProcessMultiLock write = parentWrite(client1);
            InterProcessMultiLock read =
                    InterProcessMultiLock.builder(client1).read(PARENT).build();

            write.acquire();
            assertTrue(write.acquire(5, TimeUnit.SECONDS));
            write.release();
            assertTrue(write.isAcquiredInThisProcess());
            write.release();
            assertFalse(write.isAcquiredInThisProcess());

            // read locks from a different InterProcessReadWriteLock instance do not know about this
            // thread's write lock, so downgrading only works through the same underlying lock
            InterProcessReadWriteLock rw = new InterProcessReadWriteLock(client1, PARENT);
            rw.writeLock().acquire();
            assertFalse(read.acquire(1, TimeUnit.SECONDS));
            rw.writeLock().release();
            assertTrue(read.acquire(5, TimeUnit.SECONDS));
            assertFalse(parentWrite(client2).acquire(1, TimeUnit.SECONDS));
            read.release();
        } finally {
            CloseableUtils.closeQuietly(client2);
            TestCleanState.closeAndTestClean(client1);
        }
    }

    @Test
    public void testConcurrentMixedLocks() throws Exception {
        final int threadCount = 10;
        final int iterations = 5;

        CuratorFramework client = newClient();
        ExecutorService service = Executors.newFixedThreadPool(threadCount);
        try {
            AtomicInteger parentWriters = new AtomicInteger();
            AtomicInteger childWriters = new AtomicInteger();
            AtomicInteger violations = new AtomicInteger();
            CountDownLatch start = new CountDownLatch(1);

            Future<?>[] futures = new Future<?>[threadCount];
            for (int i = 0; i < threadCount; ++i) {
                futures[i] = service.submit((Callable<Void>) () -> {
                    start.await();
                    for (int j = 0; j < iterations; ++j) {
                        boolean parent = ThreadLocalRandom.current().nextBoolean();
                        InterProcessMultiLock lock = parent ? parentWrite(client) : childWrite(client);
                        lock.acquire();
                        try {
                            if (parent) {
                                if (parentWriters.incrementAndGet() != 1 || childWriters.get() != 0) {
                                    violations.incrementAndGet();
                                }
                                Thread.sleep(10);
                                parentWriters.decrementAndGet();
                            } else {
                                if (childWriters.incrementAndGet() != 1 || parentWriters.get() != 0) {
                                    violations.incrementAndGet();
                                }
                                Thread.sleep(10);
                                childWriters.decrementAndGet();
                            }
                        } finally {
                            lock.release();
                        }
                    }
                    return null;
                });
            }

            start.countDown();
            for (Future<?> future : futures) {
                future.get(60, TimeUnit.SECONDS);
            }
            assertEquals(0, violations.get());
        } finally {
            service.shutdownNow();
            TestCleanState.closeAndTestClean(client);
        }
    }
}
