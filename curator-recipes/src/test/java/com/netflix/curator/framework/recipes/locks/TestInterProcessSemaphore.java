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
        CuratorFramework client1 = null;
        CuratorFramework client2 = null;
        try
        {
            client1 = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
            client2 = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));

            client1.start();
            client2.start();

            final InterProcessSemaphore   semaphore1 = new InterProcessSemaphore(client1, "/test");
            final InterProcessSemaphore   semaphore2 = new InterProcessSemaphore(client2, "/test");

            semaphore2.setMaxLeases(1);

            ExecutorService     service = Executors.newCachedThreadPool();

            final CountDownLatch    latch = new CountDownLatch(1);
            Future<Object>          future1 = service.submit
            (
                new Callable<Object>()
                {
                    @Override
                    public Object call() throws Exception
                    {
                        Lease lease = semaphore1.acquire(10, TimeUnit.SECONDS);
                        Assert.assertNotNull(lease);
                        latch.countDown();
                        lease = semaphore1.acquire(10, TimeUnit.SECONDS);
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
                        Assert.assertTrue(semaphore2.compareAndSetMaxLeases(1, 2));
                        return null;
                    }
                }
            );

            future1.get();
            future2.get();
        }
        finally
        {
            Closeables.closeQuietly(client1);
            Closeables.closeQuietly(client2);
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

            semaphore1 = new InterProcessSemaphore(client1, "/test");
            semaphore2 = new InterProcessSemaphore(client2, "/test");

            Assert.assertTrue(semaphore1.compareAndSetMaxLeases(0, 1));

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

        {
            CuratorFramework    client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
            client.start();
            try
            {
                InterProcessSemaphore   semaphore = new InterProcessSemaphore(client, "/test");
                semaphore.setMaxLeases(SESSION_MAX);
            }
            finally
            {
                client.close();
            }
        }

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
                                InterProcessSemaphore   semaphore = new InterProcessSemaphore(client, "/test");

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
                                        semaphore.closeAll(leases);
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

        CuratorFramework                masterClient = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        masterClient.start();
        try
        {
            InterProcessSemaphore   masterSemaphore = new InterProcessSemaphore(masterClient, "/test");
            masterSemaphore.setMaxLeases(MAX);
        }
        finally
        {
            masterClient.close();
        }

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
                            InterProcessSemaphore   semaphore = new InterProcessSemaphore(client, "/test");
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
    public void     testSimple() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            InterProcessSemaphore   semaphore = new InterProcessSemaphore(client, "/test");
            semaphore.setMaxLeases(1);
            Assert.assertNotNull(semaphore.acquire(10, TimeUnit.SECONDS));
            Assert.assertNull(semaphore.acquire(3, TimeUnit.SECONDS));
        }
        finally
        {
            client.close();
        }
    }
}
