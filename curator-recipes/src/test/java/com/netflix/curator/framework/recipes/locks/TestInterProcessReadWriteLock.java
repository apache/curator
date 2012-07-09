/*
 * Copyright 2012 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
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
import java.util.concurrent.atomic.AtomicInteger;

public class TestInterProcessReadWriteLock extends BaseClassForTests
{
    @Test
    public void     testGetParticipantNodes() throws Exception
    {
        final int               READERS = 20;
        final int               WRITERS = 8;

        CuratorFramework        client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();

            final CountDownLatch              latch = new CountDownLatch(READERS + WRITERS);
            final CountDownLatch              readLatch = new CountDownLatch(READERS);
            final InterProcessReadWriteLock   lock = new InterProcessReadWriteLock(client, "/lock");

            ExecutorService                   service = Executors.newCachedThreadPool();
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
                            latch.countDown();
                            readLatch.countDown();
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
                            Assert.assertTrue(readLatch.await(10, TimeUnit.SECONDS));
                            latch.countDown();  // must be before as there can only be one writer
                            lock.writeLock().acquire();
                            return null;
                        }
                    }
                );
            }

            Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));

            Collection<String> readers = lock.readLock().getParticipantNodes();
            Collection<String> writers = lock.writeLock().getParticipantNodes();

            Assert.assertEquals(readers.size(), READERS);
            Assert.assertEquals(writers.size(), WRITERS);
        }
        finally
        {
            Closeables.closeQuietly(client);
        }
    }

    @Test
    public void     testThatUpgradingIsDisallowed() throws Exception
    {
        CuratorFramework        client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();

            InterProcessReadWriteLock   lock = new InterProcessReadWriteLock(client, "/lock");
            lock.readLock().acquire();
            Assert.assertFalse(lock.writeLock().acquire(5, TimeUnit.SECONDS));

            lock.readLock().release();
        }
        finally
        {
            Closeables.closeQuietly(client);
        }
    }

    @Test
    public void     testThatDowngradingRespectsThreads() throws Exception
    {
        CuratorFramework        client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();

            final InterProcessReadWriteLock   lock = new InterProcessReadWriteLock(client, "/lock");
            ExecutorService                   t1 = Executors.newSingleThreadExecutor();
            ExecutorService                   t2 = Executors.newSingleThreadExecutor();

            final CountDownLatch              latch = new CountDownLatch(1);

            Future<Object>                    f1 = t1.submit
            (
                new Callable<Object>()
                {
                    @Override
                    public Object call() throws Exception
                    {
                        lock.writeLock().acquire();
                        latch.countDown();
                        return null;
                    }
                }
            );

            Future<Object>                    f2 = t2.submit
            (
                new Callable<Object>()
                {
                    @Override
                    public Object call() throws Exception
                    {
                        Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
                        Assert.assertFalse(lock.readLock().acquire(5, TimeUnit.SECONDS));
                        return null;
                    }
                }
            );

            f1.get();
            f2.get();
        }
        finally
        {
            Closeables.closeQuietly(client);
        }
    }

    @Test
    public void     testDowngrading() throws Exception
    {
        CuratorFramework        client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();

            InterProcessReadWriteLock   lock = new InterProcessReadWriteLock(client, "/lock");
            lock.writeLock().acquire();
            Assert.assertTrue(lock.readLock().acquire(5, TimeUnit.SECONDS));
            lock.writeLock().release();

            lock.readLock().release();
        }
        finally
        {
            Closeables.closeQuietly(client);
        }
    }

    @Test
    public void     testBasic() throws Exception
    {
        final int               CONCURRENCY = 8;
        final int               ITERATIONS = 100;

        final Random            random = new Random();
        final AtomicInteger     concurrentCount = new AtomicInteger(0);
        final AtomicInteger     maxConcurrentCount = new AtomicInteger(0);
        final AtomicInteger     writeCount = new AtomicInteger(0);
        final AtomicInteger     readCount = new AtomicInteger(0);

        List<Future<Void>>  futures = Lists.newArrayList();
        ExecutorService     service = Executors.newCachedThreadPool();
        for ( int i = 0; i < CONCURRENCY; ++i )
        {
            Future<Void>    future = service.submit
            (
                new Callable<Void>()
                {
                    @Override
                    public Void call() throws Exception
                    {
                        CuratorFramework        client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
                        client.start();
                        try
                        {
                            InterProcessReadWriteLock   lock = new InterProcessReadWriteLock(client, "/lock");
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
                            Closeables.closeQuietly(client);
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

        Assert.assertTrue(writeCount.get() > 0);
        Assert.assertTrue(readCount.get() > 0);
        Assert.assertTrue(maxConcurrentCount.get() > 1);
    }

    private void doLocking(InterProcessLock lock, AtomicInteger concurrentCount, AtomicInteger maxConcurrentCount, Random random, int maxAllowed) throws Exception
    {
        try
        {
            Assert.assertTrue(lock.acquire(10, TimeUnit.SECONDS));
            int     localConcurrentCount;
            synchronized(this)
            {
                localConcurrentCount = concurrentCount.incrementAndGet();
                if ( localConcurrentCount > maxConcurrentCount.get() )
                {
                    maxConcurrentCount.set(localConcurrentCount);
                }
            }

            Assert.assertTrue(localConcurrentCount <= maxAllowed, "" + localConcurrentCount);

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
}
