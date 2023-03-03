/**
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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.TestCleanState;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.KillSession;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TestInterProcessReadWriteLock extends BaseClassForTests
{
    @Test
    public void testGetParticipantNodes() throws Exception
    {
        final int READERS = 20;
        final int WRITERS = 8;

        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();

            final CountDownLatch latch = new CountDownLatch(READERS + WRITERS);
            final CountDownLatch readLatch = new CountDownLatch(READERS);
            final InterProcessReadWriteLock lock = new InterProcessReadWriteLock(client, "/lock");

            final CountDownLatch exitLatch = new CountDownLatch(1);
            ExecutorCompletionService<Void> service = new ExecutorCompletionService<Void>(Executors.newCachedThreadPool());
            for ( int i = 0; i < READERS; ++i )
            {
                service.submit
                (
                    new Callable<Void>()
                    {
                        @Override
                        public Void call() throws Exception
                        {
                            lock.readLock().acquire();
                            try
                            {
                                latch.countDown();
                                readLatch.countDown();
                                exitLatch.await();
                            }
                            finally
                            {
                                lock.readLock().release();
                            }
                            return null;
                        }
                    }
                );
            }
            for ( int i = 0; i < WRITERS; ++i )
            {
                service.submit
                (
                    new Callable<Void>()
                    {
                        @Override
                        public Void call() throws Exception
                        {
                            assertTrue(readLatch.await(10, TimeUnit.SECONDS));
                            latch.countDown();  // must be before as there can only be one writer
                            lock.writeLock().acquire();
                            try
                            {
                                exitLatch.await();
                            }
                            finally
                            {
                                lock.writeLock().release();
                            }
                            return null;
                        }
                    }
                );
            }

            assertTrue(latch.await(10, TimeUnit.SECONDS));

            Collection<String> readers = lock.readLock().getParticipantNodes();
            Collection<String> writers = lock.writeLock().getParticipantNodes();

            assertEquals(readers.size(), READERS);
            assertEquals(writers.size(), WRITERS);

            exitLatch.countDown();
            for ( int i = 0; i < (READERS + WRITERS); ++i )
            {
                service.take().get();
            }
        }
        finally
        {
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
    public void testThatUpgradingIsDisallowed() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();

            InterProcessReadWriteLock lock = new InterProcessReadWriteLock(client, "/lock");
            lock.readLock().acquire();
            assertFalse(lock.writeLock().acquire(5, TimeUnit.SECONDS));

            lock.readLock().release();
        }
        finally
        {
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
    public void testThatDowngradingRespectsThreads() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();

            final InterProcessReadWriteLock lock = new InterProcessReadWriteLock(client, "/lock");
            ExecutorService t1 = Executors.newSingleThreadExecutor();
            ExecutorService t2 = Executors.newSingleThreadExecutor();

            final CountDownLatch latch = new CountDownLatch(1);

            final CountDownLatch releaseLatch = new CountDownLatch(1);
            Future<Object> f1 = t1.submit
                (
                    new Callable<Object>()
                    {
                        @Override
                        public Object call() throws Exception
                        {
                            lock.writeLock().acquire();
                            latch.countDown();
                            try
                            {
                                releaseLatch.await();
                            }
                            finally
                            {
                                lock.writeLock().release();
                            }
                            return null;
                        }
                    }
                );

            Future<Object> f2 = t2.submit
                (
                    new Callable<Object>()
                    {
                        @Override
                        public Object call() throws Exception
                        {
                            assertTrue(latch.await(10, TimeUnit.SECONDS));
                            assertFalse(lock.readLock().acquire(5, TimeUnit.SECONDS));
                            return null;
                        }
                    }
                );

            f2.get();
            releaseLatch.countDown();
            f1.get();
        }
        finally
        {
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
    public void testDowngrading() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();

            InterProcessReadWriteLock lock = new InterProcessReadWriteLock(client, "/lock");
            lock.writeLock().acquire();
            assertTrue(lock.readLock().acquire(5, TimeUnit.SECONDS));
            lock.writeLock().release();

            lock.readLock().release();
        }
        finally
        {
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
    public void testBasic() throws Exception
    {
        final int CONCURRENCY = 8;
        final int ITERATIONS = 100;

        final Random random = new Random();
        final AtomicInteger concurrentCount = new AtomicInteger(0);
        final AtomicInteger maxConcurrentCount = new AtomicInteger(0);
        final AtomicInteger writeCount = new AtomicInteger(0);
        final AtomicInteger readCount = new AtomicInteger(0);

        List<Future<Void>> futures = Lists.newArrayList();
        ExecutorService service = Executors.newCachedThreadPool();
        for ( int i = 0; i < CONCURRENCY; ++i )
        {
            Future<Void> future = service.submit
                (
                    new Callable<Void>()
                    {
                        @Override
                        public Void call() throws Exception
                        {
                            CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
                            client.start();
                            try
                            {
                                InterProcessReadWriteLock lock = new InterProcessReadWriteLock(client, "/lock");
                                for ( int i = 0; i < ITERATIONS; ++i )
                                {
                                    if ( random.nextInt(100) < 10 )
                                    {
                                        doLocking(lock.writeLock(), concurrentCount, maxConcurrentCount, random, 1);
                                        writeCount.incrementAndGet();
                                    }
                                    else
                                    {
                                        doLocking(lock.readLock(), concurrentCount, maxConcurrentCount, random, Integer.MAX_VALUE);
                                        readCount.incrementAndGet();
                                    }
                                }
                            }
                            finally
                            {
                                TestCleanState.closeAndTestClean(client);
                            }
                            return null;
                        }
                    }
                );
            futures.add(future);
        }

        for ( Future<Void> future : futures )
        {
            future.get();
        }

        System.out.println("Writes: " + writeCount.get() + " - Reads: " + readCount.get() + " - Max Reads: " + maxConcurrentCount.get());

        assertTrue(writeCount.get() > 0);
        assertTrue(readCount.get() > 0);
        assertTrue(maxConcurrentCount.get() > 1);
    }

    @Test
    public void testSetNodeData() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));

        try
        {
            client.start();

            final byte[] nodeData = new byte[]{1, 2, 3, 4};

            InterProcessReadWriteLock lock = new InterProcessReadWriteLock(client, "/lock", nodeData);

            // mutate passed-in node data, lock has made copy
            nodeData[0] = 5;

            lock.writeLock().acquire();

            List<String> children = client.getChildren().forPath("/lock");
            assertEquals(1, children.size());

            byte dataInZk[] = client.getData().forPath("/lock/" + children.get(0));
            assertNotNull(dataInZk);
            assertArrayEquals(new byte[]{1, 2, 3, 4}, dataInZk);

            lock.writeLock().release();
        }
        finally
        {
            TestCleanState.closeAndTestClean(client);
        }
    }

    private void doLocking(InterProcessLock lock, AtomicInteger concurrentCount, AtomicInteger maxConcurrentCount, Random random, int maxAllowed) throws Exception
    {
        try
        {
            assertTrue(lock.acquire(10, TimeUnit.SECONDS));
            int localConcurrentCount;
            synchronized(this)
            {
                localConcurrentCount = concurrentCount.incrementAndGet();
                if ( localConcurrentCount > maxConcurrentCount.get() )
                {
                    maxConcurrentCount.set(localConcurrentCount);
                }
            }

            assertTrue(localConcurrentCount <= maxAllowed, "" + localConcurrentCount);

            Thread.sleep(random.nextInt(9) + 1);
        }
        finally
        {
            synchronized(this)
            {
                concurrentCount.decrementAndGet();
                lock.release();
            }
        }
    }

    @Test
    public void testLockPath() throws Exception
    {
        CuratorFramework client1 = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        CuratorFramework client2 = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client1.start();
            client2.start();
            InterProcessReadWriteLock lock1 = new InterProcessReadWriteLock(client1, "/lock");
            InterProcessReadWriteLock lock2 = new InterProcessReadWriteLock(client2, "/lock");
            lock1.writeLock().acquire();
            KillSession.kill(client1.getZookeeperClient().getZooKeeper());
            lock2.readLock().acquire();
            try {
                client1.getData().forPath(lock1.writeLock().getLockPath());
                fail("expected not to find node");
            } catch (KeeperException.NoNodeException ignored) {
            }
            lock2.readLock().release();
            lock1.writeLock().release();
        }
        finally
        {
            TestCleanState.closeAndTestClean(client2);
            TestCleanState.closeAndTestClean(client1);
        }
    }
}
