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

import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.imps.TestCleanState;
import org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings({"SynchronizationOnLocalVariableOrMethodParameter"})
public class TestInterProcessSemaphore extends BaseClassForTests
{
    @Test
    public void testAcquireAfterLostServer() throws Exception
    {
        // CURATOR-335

        final String SEMAPHORE_PATH = "/test";
        final int MAX_SEMAPHORES = 1;
        final int NUM_CLIENTS = 10;

        ExecutorService executor = Executors.newFixedThreadPool(NUM_CLIENTS);

        final Timing timing = new Timing();

        final CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.forWaiting().milliseconds(), timing.connection(), new RetryOneTime(1));  // long session time on purpose
        try
        {
            client.start();

            InterProcessSemaphoreV2.debugAcquireLatch = new CountDownLatch(1);  // cause one of the semaphores to create its node and then wait
            InterProcessSemaphoreV2.debugFailedGetChildrenLatch = new CountDownLatch(1);    // semaphore will notify when getChildren() fails
            final CountDownLatch isReadyLatch = new CountDownLatch(NUM_CLIENTS);
            final BlockingQueue<Boolean> acquiredQueue = Queues.newLinkedBlockingQueue();
            Runnable runner = new Runnable()
            {
                @Override
                public void run()
                {
                    while ( !Thread.currentThread().isInterrupted() )
                    {
                        InterProcessSemaphoreV2 semaphore = new InterProcessSemaphoreV2(client, SEMAPHORE_PATH, MAX_SEMAPHORES);
                        Lease lease = null;
                        try
                        {
                            isReadyLatch.countDown();
                            lease = semaphore.acquire();
                            acquiredQueue.add(true);
                            timing.sleepABit();
                        }
                        catch ( InterruptedException e )
                        {
                            Thread.currentThread().interrupt();
                            break;
                        }
                        catch ( KeeperException e )
                        {
                            try
                            {
                                timing.sleepABit();
                            }
                            catch ( InterruptedException e2 )
                            {
                                Thread.currentThread().interrupt();
                                break;
                            }
                        }
                        catch ( Exception ignore )
                        {
                            // ignore
                        }
                        finally
                        {
                            if ( lease != null )
                            {
                                semaphore.returnLease(lease);
                            }
                        }
                    }
                }
            };
            for ( int i = 0; i < NUM_CLIENTS; ++i )
            {
                executor.execute(runner);
            }
            Assert.assertTrue(timing.awaitLatch(isReadyLatch));
            timing.sleepABit();

            final CountDownLatch lostLatch = new CountDownLatch(1);
            final CountDownLatch restartedLatch = new CountDownLatch(1);
            client.getConnectionStateListenable().addListener(new ConnectionStateListener()
            {
                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState)
                {
                    if ( newState == ConnectionState.LOST )
                    {
                        lostLatch.countDown();
                    }
                    else if ( newState == ConnectionState.RECONNECTED  )
                    {
                        restartedLatch.countDown();
                    }
                }
            });
      
            server.stop();
            Assert.assertTrue(timing.multiple(1.25).awaitLatch(lostLatch));
            InterProcessSemaphoreV2.debugAcquireLatch.countDown();  // the waiting semaphore proceeds to getChildren - which should fail
            Assert.assertTrue(timing.awaitLatch(InterProcessSemaphoreV2.debugFailedGetChildrenLatch));  // wait until getChildren fails

            server.restart();

            Assert.assertTrue(timing.awaitLatch(restartedLatch));
            for ( int i = 0; i < NUM_CLIENTS; ++i )
            {
                // acquires should continue as normal after server restart
                Boolean polled = acquiredQueue.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS);
                if ( (polled == null) || !polled )
                {
                    Assert.fail("Semaphores not reacquired after restart");
                }
            }
        }
        finally
        {
            executor.shutdownNow();
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testThreadedLeaseIncrease() throws Exception
    {
        final Timing timing = new Timing();
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        try
        {
            client.start();

            final SharedCount count = new SharedCount(client, "/foo/count", 1);
            count.start();

            final InterProcessSemaphoreV2 semaphore = new InterProcessSemaphoreV2(client, "/test", count);

            ExecutorService service = Executors.newCachedThreadPool();

            final CountDownLatch latch1 = new CountDownLatch(1);
            final CountDownLatch latch2 = new CountDownLatch(1);
            Future<Object> future1 = service.submit
                (
                    new Callable<Object>()
                    {
                        @Override
                        public Object call() throws Exception
                        {
                            Lease lease = semaphore.acquire(timing.seconds(), TimeUnit.SECONDS);
                            Assert.assertNotNull(lease);
                            latch1.countDown();
                            lease = semaphore.acquire(timing.forWaiting().seconds(), TimeUnit.SECONDS);
                            Assert.assertNotNull(lease);
                            latch2.countDown();
                            return null;
                        }
                    }
                );
            Future<Object> future2 = service.submit
                (
                    new Callable<Object>()
                    {
                        @Override
                        public Object call() throws Exception
                        {
                            Assert.assertTrue(latch1.await(timing.forWaiting().seconds(), TimeUnit.SECONDS));
                            timing.sleepABit(); // make sure second acquire is waiting
                            Assert.assertTrue(count.trySetCount(2));
                            //Make sure second acquire takes less than full waiting time:
                            timing.sleepABit();
                            Assert.assertTrue(latch2.await(0, TimeUnit.SECONDS));
                            return null;
                        }
                    }
                );

            future1.get();
            future2.get();

            count.close();
        }
        finally
        {
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
    public void testClientClose() throws Exception
    {
        final Timing timing = new Timing();
        CuratorFramework client1 = null;
        CuratorFramework client2 = null;
        InterProcessSemaphoreV2 semaphore1;
        InterProcessSemaphoreV2 semaphore2;
        try
        {
            client1 = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
            client2 = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));

            client1.start();
            client2.start();

            semaphore1 = new InterProcessSemaphoreV2(client1, "/test", 1);
            semaphore2 = new InterProcessSemaphoreV2(client2, "/test", 1);

            Lease lease = semaphore2.acquire(timing.forWaiting().seconds(), TimeUnit.SECONDS);
            Assert.assertNotNull(lease);
            lease.close();

            lease = semaphore1.acquire(10, TimeUnit.SECONDS);
            Assert.assertNotNull(lease);

            client1.close();    // should release any held leases
            client1 = null;

            Assert.assertNotNull(semaphore2.acquire(timing.forWaiting().seconds(), TimeUnit.SECONDS));
        }
        finally
        {
            TestCleanState.closeAndTestClean(client1);
            TestCleanState.closeAndTestClean(client2);
        }
    }

    @Test
    public void testMaxPerSession() throws Exception
    {
        final int CLIENT_QTY = 10;
        final int LOOP_QTY = 100;
        final Random random = new Random();
        final int SESSION_MAX = random.nextInt(75) + 25;
        final Timing timing = new Timing();

        List<Future<Object>> futures = Lists.newArrayList();
        ExecutorService service = Executors.newCachedThreadPool();
        final Counter counter = new Counter();
        final AtomicInteger available = new AtomicInteger(SESSION_MAX);
        for ( int i = 0; i < CLIENT_QTY; ++i )
        {
            futures.add
                (
                    service.submit
                        (
                            new Callable<Object>()
                            {
                                @Override
                                public Object call() throws Exception
                                {
                                    CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
                                    client.start();
                                    try
                                    {
                                        InterProcessSemaphoreV2 semaphore = new InterProcessSemaphoreV2(client, "/test", SESSION_MAX);

                                        for ( int i = 0; i < LOOP_QTY; ++i )
                                        {
                                            long start = System.currentTimeMillis();
                                            int thisQty;
                                            synchronized(available)
                                            {
                                                if ( (System.currentTimeMillis() - start) > 10000 )
                                                {
                                                    throw new TimeoutException();
                                                }
                                                while ( available.get() == 0 )
                                                {
                                                    available.wait(timing.forWaiting().milliseconds());
                                                }

                                                thisQty = (available.get() > 1) ? (random.nextInt(available.get()) + 1) : 1;

                                                available.addAndGet(-1 * thisQty);
                                                Assert.assertTrue(available.get() >= 0);
                                            }
                                            Collection<Lease> leases = semaphore.acquire(thisQty, timing.forWaiting().seconds(), TimeUnit.SECONDS);
                                            Assert.assertNotNull(leases);
                                            try
                                            {
                                                synchronized(counter)
                                                {
                                                    counter.currentCount += thisQty;
                                                    if ( counter.currentCount > counter.maxCount )
                                                    {
                                                        counter.maxCount = counter.currentCount;
                                                    }
                                                }
                                                Thread.sleep(random.nextInt(25));
                                            }
                                            finally
                                            {
                                                synchronized(counter)
                                                {
                                                    counter.currentCount -= thisQty;
                                                }
                                                semaphore.returnAll(leases);
                                                synchronized(available)
                                                {
                                                    available.addAndGet(thisQty);
                                                    available.notifyAll();
                                                }
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
                        )
                );
        }

        for ( Future<Object> f : futures )
        {
            f.get();
        }

        synchronized(counter)
        {
            Assert.assertTrue(counter.currentCount == 0);
            Assert.assertTrue(counter.maxCount > 0);
            Assert.assertTrue(counter.maxCount <= SESSION_MAX);
            System.out.println(counter.maxCount);
        }
    }

    @Test
    public void testRelease1AtATime() throws Exception
    {
        final Timing timing = new Timing();
        final int CLIENT_QTY = 10;
        final int MAX = CLIENT_QTY / 2;
        final AtomicInteger maxLeases = new AtomicInteger(0);
        final AtomicInteger activeQty = new AtomicInteger(0);
        final AtomicInteger uses = new AtomicInteger(0);

        List<Future<Object>> futures = Lists.newArrayList();
        ExecutorService service = Executors.newFixedThreadPool(CLIENT_QTY);
        for ( int i = 0; i < CLIENT_QTY; ++i )
        {
            Future<Object> f = service.submit
                (
                    new Callable<Object>()
                    {
                        @Override
                        public Object call() throws Exception
                        {
                            CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
                            client.start();
                            try
                            {
                                InterProcessSemaphoreV2 semaphore = new InterProcessSemaphoreV2(client, "/test", MAX);
                                Lease lease = semaphore.acquire(timing.forWaiting().seconds(), TimeUnit.SECONDS);
                                Assert.assertNotNull(lease);
                                uses.incrementAndGet();
                                try
                                {
                                    synchronized(maxLeases)
                                    {
                                        int qty = activeQty.incrementAndGet();
                                        if ( qty > maxLeases.get() )
                                        {
                                            maxLeases.set(qty);
                                        }
                                    }

                                    timing.sleepABit();
                                }
                                finally
                                {
                                    activeQty.decrementAndGet();
                                    lease.close();
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
            futures.add(f);
        }

        for ( Future<Object> f : futures )
        {
            f.get();
        }

        Assert.assertEquals(uses.get(), CLIENT_QTY);
        Assert.assertEquals(maxLeases.get(), MAX);
    }

    @Test
    public void testReleaseInChunks() throws Exception
    {
        final Timing timing = new Timing();
        final int MAX_LEASES = 11;
        final int THREADS = 100;

        final CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        client.start();
        try
        {
            final Stepper latch = new Stepper();
            final Random random = new Random();
            final Counter counter = new Counter();
            ExecutorService service = Executors.newCachedThreadPool();
            ExecutorCompletionService<Object> completionService = new ExecutorCompletionService<Object>(service);
            for ( int i = 0; i < THREADS; ++i )
            {
                completionService.submit
                    (
                        new Callable<Object>()
                        {
                            @Override
                            public Object call() throws Exception
                            {
                                InterProcessSemaphoreV2 semaphore = new InterProcessSemaphoreV2(client, "/test", MAX_LEASES);
                                Lease lease = semaphore.acquire(timing.forWaiting().seconds(), TimeUnit.SECONDS);
                                if ( lease == null )
                                {
                                    throw new Exception("timed out");
                                }
                                try
                                {
                                    synchronized(counter)
                                    {
                                        ++counter.currentCount;
                                        if ( counter.currentCount > counter.maxCount )
                                        {
                                            counter.maxCount = counter.currentCount;
                                        }
                                        counter.notifyAll();
                                    }

                                    latch.await();
                                }
                                finally
                                {
                                    synchronized(counter)
                                    {
                                        --counter.currentCount;
                                    }
                                    semaphore.returnLease(lease);
                                }
                                return null;
                            }
                        }
                    );
            }

            int remaining = THREADS;
            while ( remaining > 0 )
            {
                int times = Math.min(random.nextInt(5) + 1, remaining);
                latch.countDown(times);
                remaining -= times;
                Thread.sleep(random.nextInt(100) + 1);
            }

            for ( int i = 0; i < THREADS; ++i )
            {
                completionService.take();
            }

            timing.sleepABit();
            synchronized(counter)
            {
                Assert.assertTrue(counter.currentCount == 0);
                Assert.assertTrue(counter.maxCount > 0);
                Assert.assertTrue(counter.maxCount <= MAX_LEASES);
                System.out.println(counter.maxCount);
            }
        }
        finally
        {
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
    public void testThreads() throws Exception
    {
        final int THREAD_QTY = 10;

        Timing timing = new Timing();
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        client.start();
        try
        {
            final InterProcessSemaphoreV2 semaphore = new InterProcessSemaphoreV2(client, "/test", 1);
            ExecutorService service = Executors.newFixedThreadPool(THREAD_QTY);
            for ( int i = 0; i < THREAD_QTY; ++i )
            {
                service.submit
                    (
                        new Callable<Object>()
                        {
                            @Override
                            public Object call() throws Exception
                            {
                                Lease lease = semaphore.acquire();
                                try
                                {
                                    Thread.sleep(1);
                                }
                                finally
                                {
                                    lease.close();
                                }
                                return null;
                            }
                        }
                    );
            }
            service.shutdown();
            Assert.assertTrue(service.awaitTermination(10, TimeUnit.SECONDS));
        }
        finally
        {
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
    public void testSimple() throws Exception
    {
        Timing timing = new Timing();
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        client.start();
        try
        {
            InterProcessSemaphoreV2 semaphore = new InterProcessSemaphoreV2(client, "/test", 1);
            Assert.assertNotNull(semaphore.acquire(timing.forWaiting().seconds(), TimeUnit.SECONDS));
            Assert.assertNull(semaphore.acquire(timing.forWaiting().seconds(), TimeUnit.SECONDS));
        }
        finally
        {
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
    public void testSimple2() throws Exception
    {
        final int MAX_LEASES = 3;
        Timing timing = new Timing();

        List<Lease> leases = Lists.newArrayList();
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        client.start();
        try
        {
            for ( int i = 0; i < MAX_LEASES; ++i )
            {
                InterProcessSemaphoreV2 semaphore = new InterProcessSemaphoreV2(client, "/test", MAX_LEASES);
                Lease lease = semaphore.acquire(timing.forWaiting().seconds(), TimeUnit.SECONDS);
                Assert.assertNotNull(lease);
                leases.add(lease);
            }

            InterProcessSemaphoreV2 semaphore = new InterProcessSemaphoreV2(client, "/test", MAX_LEASES);
            Lease lease = semaphore.acquire(timing.forWaiting().seconds(), TimeUnit.SECONDS);
            Assert.assertNull(lease);

            leases.remove(0).close();
            Assert.assertNotNull(semaphore.acquire(timing.forWaiting().seconds(), TimeUnit.SECONDS));
        }
        finally
        {
            for ( Lease l : leases )
            {
                CloseableUtils.closeQuietly(l);
            }
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
    public void testGetParticipantNodes() throws Exception
    {
        final int LEASES = 3;

        Timing timing = new Timing();
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        List<Lease> leases = Lists.newArrayList();
        client.start();
        try
        {
            InterProcessSemaphoreV2 semaphore = new InterProcessSemaphoreV2(client, "/test", LEASES);
            for ( int i = 0; i < LEASES; ++i )
            {
                leases.add(semaphore.acquire());
            }

            Assert.assertEquals(semaphore.getParticipantNodes().size(), LEASES);
        }
        finally
        {
            for ( Lease l : leases )
            {
                CloseableUtils.closeQuietly(l);
            }
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
    public void testChildReaperCleansUpLockNodes() throws Exception
    {
        Timing timing = new Timing();
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        client.start();

        ChildReaper childReaper = null;
        try
        {
            InterProcessSemaphoreV2 semaphore = new InterProcessSemaphoreV2(client, "/test/lock", 1);
            semaphore.returnLease(semaphore.acquire(timing.forWaiting().seconds(), TimeUnit.SECONDS));

            Assert.assertTrue(client.getChildren().forPath("/test").size() > 0);

            childReaper = new ChildReaper(
                client,
                "/test",
                Reaper.Mode.REAP_UNTIL_GONE,
                ChildReaper.newExecutorService(),
                1,
                "/test-leader",
                InterProcessSemaphoreV2.LOCK_SCHEMA
            );
            childReaper.start();

            timing.forWaiting().sleepABit();

            List<String> children = client.getChildren().forPath("/test");

            Assert.assertEquals(children.size(), 0, "All children of /test should have been reaped");
        }
        finally
        {
            CloseableUtils.closeQuietly(childReaper);
            CloseableUtils.closeQuietly(client);
        }

    }

    @Test
    public void testNoOrphanedNodes() throws Exception
    {
        final Timing timing = new Timing();
        final ExecutorService executor = Executors.newFixedThreadPool(1);
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        client.start();
        try
        {
            final InterProcessSemaphoreV2 semaphore = new InterProcessSemaphoreV2(client, "/test", 1);
            Lease lease = semaphore.acquire(timing.forWaiting().seconds(), TimeUnit.SECONDS);
            Assert.assertNotNull(lease);
            final List<String> childNodes = client.getChildren().forPath("/test/leases");
            Assert.assertEquals(childNodes.size(), 1);

            final CountDownLatch nodeCreatedLatch = new CountDownLatch(1);
            client.getChildren().usingWatcher(new CuratorWatcher()
            {
                @Override
                public void process(WatchedEvent event) throws Exception
                {
                    if ( event.getType() == Watcher.Event.EventType.NodeCreated )
                    {
                        nodeCreatedLatch.countDown();
                    }
                }
            }).forPath("/test/leases");

            final Future<Lease> leaseFuture = executor.submit(new Callable<Lease>()
            {
                @Override
                public Lease call() throws Exception
                {
                    return semaphore.acquire(timing.forWaiting().multiple(2).seconds(), TimeUnit.SECONDS);
                }
            });

            // wait for second lease to create its node
            timing.awaitLatch(nodeCreatedLatch);
            String newNode = null;
            for ( String c : client.getChildren().forPath("/test/leases") )
            {
                if ( !childNodes.contains(c) )
                {
                    newNode = c;
                }
            }
            Assert.assertNotNull(newNode);

            // delete the ephemeral node to trigger a retry
            client.delete().forPath("/test/leases/" + newNode);

            // release first lease so second one can be acquired
            lease.close();
            lease = leaseFuture.get();
            Assert.assertNotNull(lease);
            lease.close();
            Assert.assertEquals(client.getChildren().forPath("/test/leases").size(), 0);

            // no more lease exist. must be possible to acquire a new one
            Assert.assertNotNull(semaphore.acquire(timing.forWaiting().seconds(), TimeUnit.SECONDS));
        }
        finally
        {
            executor.shutdownNow();
            TestCleanState.closeAndTestClean(client);
        }
    }
}
