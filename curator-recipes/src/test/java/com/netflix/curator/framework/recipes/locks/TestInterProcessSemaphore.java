/*
 *
 *  Copyright 2011 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package com.netflix.curator.framework.recipes.locks;

import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.recipes.BaseClassForTests;
import com.netflix.curator.framework.recipes.shared.SharedCount;
import com.netflix.curator.retry.RetryOneTime;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
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
    public void testThreadedLeaseIncrease() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();

            final SharedCount             count = new SharedCount(client, "/foo/count", 1);
            count.start();

            final InterProcessSemaphore   semaphore = new InterProcessSemaphore(client, "/test", count);

            ExecutorService     service = Executors.newCachedThreadPool();

            final CountDownLatch    latch = new CountDownLatch(1);
            Future<Object>          future1 = service.submit
                (
                    new Callable<Object>()
                    {
                        @Override
                        public Object call() throws Exception
                        {
                            Lease lease = semaphore.acquire(10, TimeUnit.SECONDS);
                            Assert.assertNotNull(lease);
                            latch.countDown();
                            lease = semaphore.acquire(10, TimeUnit.SECONDS);
                            Assert.assertNotNull(lease);
                            return null;
                        }
                    }
                );
            Future<Object>          future2 = service.submit
            (
                new Callable<Object>()
                {
                    @Override
                    public Object call() throws Exception
                    {
                        Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
                        Thread.sleep(1000); // make sure second acquire is waiting
                        Assert.assertTrue(count.trySetCount(2));
                        return null;
                    }
                }
            );

            future1.get();
            future2.get();
        }
        finally
        {
            Closeables.closeQuietly(client);
        }
    }

    @Test
    public void testClientClose() throws Exception
    {
        CuratorFramework client1 = null;
        CuratorFramework client2 = null;
        InterProcessSemaphore semaphore1;
        InterProcessSemaphore semaphore2;
        try
        {
            client1 = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
            client2 = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));

            client1.start();
            client2.start();

            semaphore1 = new InterProcessSemaphore(client1, "/test", 1);
            semaphore2 = new InterProcessSemaphore(client2, "/test", 1);

            Lease lease = semaphore2.acquire(10, TimeUnit.SECONDS);
            Assert.assertNotNull(lease);
            lease.close();

            lease = semaphore1.acquire(10, TimeUnit.SECONDS);
            Assert.assertNotNull(lease);

            client1.close();    // should release any held leases
            client1 = null;
            
            Assert.assertNotNull(semaphore2.acquire(10, TimeUnit.SECONDS));
        }
        finally
        {
            Closeables.closeQuietly(client1);
            Closeables.closeQuietly(client2);
        }
    }

    @Test
    public void     testMaxPerSession() throws Exception
    {
        final int             CLIENT_QTY = 10;
        final int             LOOP_QTY = 100;
        final Random          random = new Random();
        final int             SESSION_MAX = random.nextInt(75) + 25;

        List<Future<Object>>  futures = Lists.newArrayList();
        ExecutorService       service = Executors.newCachedThreadPool();
        final Counter         counter = new Counter();
        final AtomicInteger   available = new AtomicInteger(SESSION_MAX);
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
                            CuratorFramework    client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
                            client.start();
                            try
                            {
                                InterProcessSemaphore   semaphore = new InterProcessSemaphore(client, "/test", SESSION_MAX);

                                for ( int i = 0; i < LOOP_QTY; ++i )
                                {
                                    long    start = System.currentTimeMillis();
                                    int     thisQty;
                                    synchronized(available)
                                    {
                                        if ( (System.currentTimeMillis() - start) > 10000 )
                                        {
                                            throw new TimeoutException();
                                        }
                                        while ( available.get() == 0 )
                                        {
                                            available.wait(10000);
                                        }

                                        thisQty = (available.get() > 1) ? (random.nextInt(available.get()) + 1) : 1;

                                        available.addAndGet(-1 * thisQty);
                                        Assert.assertTrue(available.get() >= 0);
                                    }
                                    Collection<Lease> leases = semaphore.acquire(thisQty, 10, TimeUnit.SECONDS);
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
                                client.close();
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
    public void     testRelease1AtATime() throws Exception
    {
        final int               CLIENT_QTY = 10;
        final int               MAX = CLIENT_QTY / 2;
        final AtomicInteger     maxLeases = new AtomicInteger(0);
        final AtomicInteger     activeQty = new AtomicInteger(0);
        final AtomicInteger     uses = new AtomicInteger(0);

        List<Future<Object>>    futures = Lists.newArrayList();
        ExecutorService         service = Executors.newFixedThreadPool(CLIENT_QTY);
        for ( int i = 0; i < CLIENT_QTY; ++i )
        {
            Future<Object>      f = service.submit
            (
                new Callable<Object>()
                {
                    @Override
                    public Object call() throws Exception
                    {
                        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
                        client.start();
                        try
                        {
                            InterProcessSemaphore   semaphore = new InterProcessSemaphore(client, "/test", MAX);
                            Lease lease = semaphore.acquire(10, TimeUnit.SECONDS);
                            Assert.assertNotNull(lease);
                            uses.incrementAndGet();
                            try
                            {
                                synchronized(maxLeases)
                                {
                                    int         qty = activeQty.incrementAndGet();
                                    if ( qty > maxLeases.get() )
                                    {
                                        maxLeases.set(qty);
                                    }
                                }

                                Thread.sleep(500);
                            }
                            finally
                            {
                                activeQty.decrementAndGet();
                                lease.close();
                            }
                        }
                        finally
                        {
                            client.close();
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
    public void     testReleaseInChunks() throws Exception
    {
        final int       MAX_LEASES = 11;
        final int       THREADS = 100;

        final CuratorFramework    client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            final Stepper         latch = new Stepper();
            final Random          random = new Random();
            final Counter         counter = new Counter();
            ExecutorService       service = Executors.newCachedThreadPool();
            for ( int i = 0; i < THREADS; ++i )
            {
                service.submit
                (
                    new Callable<Object>()
                    {
                        @Override
                        public Object call() throws Exception
                        {
                            InterProcessSemaphore      semaphore = new InterProcessSemaphore(client, "/test", MAX_LEASES);
                            Lease lease = semaphore.acquire();
                            try
                            {
                                synchronized(counter)
                                {
                                    ++counter.currentCount;
                                    if ( counter.currentCount > counter.maxCount )
                                    {
                                        counter.maxCount = counter.currentCount;
                                    }
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

            int     remaining = THREADS;
            while ( remaining > 0 )
            {
                int times = Math.min(random.nextInt(5) + 1, remaining);
                latch.countDown(times);
                remaining -= times;
                Thread.sleep(random.nextInt(100) + 1);
            }
            Thread.sleep(1000);

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
            client.close();
        }
    }
    
    @Test
    public void     testThreads() throws Exception
    {
        final int        THREAD_QTY = 10;

        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            final InterProcessSemaphore   semaphore = new InterProcessSemaphore(client, "/test", 1);
            ExecutorService               service = Executors.newFixedThreadPool(THREAD_QTY);
            for ( int i = 0; i < THREAD_QTY; ++i )
            {
                service.submit
                (
                    new Callable<Object>()
                    {
                        @Override
                        public Object call() throws Exception
                        {
                            Lease   lease = semaphore.acquire();
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
            client.close();
        }
    }


    @Test
    public void     testSimple() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            InterProcessSemaphore   semaphore = new InterProcessSemaphore(client, "/test", 1);
            Assert.assertNotNull(semaphore.acquire(10, TimeUnit.SECONDS));
            Assert.assertNull(semaphore.acquire(3, TimeUnit.SECONDS));
        }
        finally
        {
            client.close();
        }
    }

    @Test
    public void     testSimple2() throws Exception
    {
        final int       MAX_LEASES = 3;

        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            List<Lease>        leases = Lists.newArrayList();
            for ( int i = 0; i < MAX_LEASES; ++i )
            {
                InterProcessSemaphore      semaphore = new InterProcessSemaphore(client, "/test", MAX_LEASES);
                Lease                      lease = semaphore.acquire(10, TimeUnit.SECONDS);
                Assert.assertNotNull(lease);
                leases.add(lease);
            }

            InterProcessSemaphore      semaphore = new InterProcessSemaphore(client, "/test", MAX_LEASES);
            Lease lease = semaphore.acquire(3, TimeUnit.SECONDS);
            Assert.assertNull(lease);

            leases.remove(0).close();
            Assert.assertNotNull(semaphore.acquire(10, TimeUnit.SECONDS));
        }
        finally
        {
            client.close();
        }
    }
}
