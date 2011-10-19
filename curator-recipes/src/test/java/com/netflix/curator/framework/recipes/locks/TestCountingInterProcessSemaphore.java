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
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.recipes.BaseClassForTests;
import com.netflix.curator.retry.RetryOneTime;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings({"SynchronizationOnLocalVariableOrMethodParameter"})
public class TestCountingInterProcessSemaphore extends BaseClassForTests
{
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
                CountingInterProcessSemaphore   semaphore = new CountingInterProcessSemaphore(client, "/test");
                semaphore.release(SESSION_MAX);
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
                                CountingInterProcessSemaphore   semaphore = new CountingInterProcessSemaphore(client, "/test");

                                for ( int i = 0; i < LOOP_QTY; ++i )
                                {
                                    int     thisQty;
                                    synchronized(available)
                                    {
                                        while ( available.get() == 0 )
                                        {
                                            available.wait();
                                        }

                                        thisQty = (available.get() > 1) ? (random.nextInt(available.get()) + 1) : 1;

                                        available.addAndGet(-1 * thisQty);
                                        Assert.assertTrue(available.get() >= 0);
                                    }
                                    Assert.assertTrue(semaphore.acquire(thisQty, 10, TimeUnit.SECONDS));
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
                                        semaphore.release(thisQty);
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
    public void     testReleaseInChunks() throws Exception
    {
        final int       MAX_LEASES = 11;
        final int       THREADS = 100;

        final CuratorFramework    client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            CountingInterProcessSemaphore   masterSemaphore = new CountingInterProcessSemaphore(client, "/test");
            for ( int i = 0; i < MAX_LEASES; ++i )
            {
                masterSemaphore.release();
            }

            final Stepper         latch = new Stepper();
            final Random random = new Random();
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
                            CountingInterProcessSemaphore      semaphore = new CountingInterProcessSemaphore(client, "/test");
                            semaphore.acquire();
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
                                semaphore.release();
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
            CountingInterProcessSemaphore   masterSemaphore = new CountingInterProcessSemaphore(masterClient, "/test");
            for ( int i = 0; i < MAX; ++i )
            {
                masterSemaphore.release();
            }
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
                            CountingInterProcessSemaphore   semaphore = new CountingInterProcessSemaphore(client, "/test");
                            Assert.assertTrue(semaphore.acquire(10, TimeUnit.SECONDS));
                            uses.incrementAndGet();
                            int                             qty = activeQty.incrementAndGet();
                            try
                            {
                                synchronized(maxLeases)
                                {
                                    if ( qty > maxLeases.get() )
                                    {
                                        maxLeases.set(qty);
                                    }
                                }

                                Thread.sleep(5000);
                            }
                            finally
                            {
                                activeQty.decrementAndGet();
                                semaphore.release();
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

        masterClient = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        masterClient.start();
        try
        {
            CountingInterProcessSemaphore   masterSemaphore = new CountingInterProcessSemaphore(masterClient, "/test");
            Assert.assertEquals(masterSemaphore.getCurrentLeases(), MAX);
        }
        finally
        {
            masterClient.close();
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
            final CountingInterProcessSemaphore   semaphore = new CountingInterProcessSemaphore(client, "/test");
            ExecutorService                       service = Executors.newFixedThreadPool(THREAD_QTY);
            for ( int i = 0; i < THREAD_QTY; ++i )
            {
                service.submit
                (
                    new Callable<Object>()
                    {
                        @Override
                        public Object call() throws Exception
                        {
                            semaphore.release();
                            Thread.sleep(1);
                            semaphore.acquire();
                            return null;
                        }
                    }
                );
            }
            service.shutdown();
            Assert.assertTrue(service.awaitTermination(10, TimeUnit.SECONDS));
            Assert.assertEquals(semaphore.getCurrentLeases(), 0);
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
            CountingInterProcessSemaphore   semaphore = new CountingInterProcessSemaphore(client, "/test");
            semaphore.release();
            Assert.assertTrue(semaphore.acquire(10, TimeUnit.SECONDS));
            Assert.assertFalse(semaphore.acquire(3, TimeUnit.SECONDS));
        }
        finally
        {
            client.close();
        }
    }
}
