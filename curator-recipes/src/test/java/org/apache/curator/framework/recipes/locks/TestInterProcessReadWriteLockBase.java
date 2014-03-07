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

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.BaseClassForTests;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class TestInterProcessReadWriteLockBase extends BaseClassForTests
{
    @Test
    public void testWriterAgainstConstantReaders() throws Exception
    {
        final int CONCURRENCY = 8;
        final int WRITER_ATTEMPTS = 10;

        ExecutorService service = Executors.newCachedThreadPool();
        ExecutorCompletionService<Void> completionService = new ExecutorCompletionService<Void>(service);
        for ( int i = 0; i < CONCURRENCY; ++i )
        {
            completionService.submit(new Callable<Void>()
            {
                @Override
                public Void call() throws Exception
                {
                    CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
                    client.start();
                    try
                    {
                        InterProcessReadWriteLockBase lock = newLock(client, "/lock");
                        try
                        {
                            while ( !Thread.currentThread().isInterrupted() )
                            {
                                lock.readLock().acquire();
                                try
                                {
                                    Thread.sleep(100);
                                }
                                finally
                                {
                                    lock.readLock().release();
                                }
                            }
                        }
                        catch ( InterruptedException dummy )
                        {
                            Thread.currentThread().interrupt();
                        }
                    }
                    finally
                    {
                        CloseableUtils.closeQuietly(client);
                    }
                    return null;
                }
            });
        }

        new Timing().sleepABit();

        final AtomicInteger writerCount = new AtomicInteger();
        Future<Void> writerThread = completionService.submit(new Callable<Void>()
        {
            @Override
            public Void call() throws Exception
            {
                CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
                client.start();
                try
                {
                    InterProcessReadWriteLockBase lock = newLock(client, "/lock");
                    for ( int i = 0; i < WRITER_ATTEMPTS; ++i )
                    {
                        if ( !lock.writeLock().acquire(10, TimeUnit.SECONDS) )
                        {
                            throw new Exception("Could not get write lock");
                        }
                        try
                        {
                            writerCount.incrementAndGet();
                            Thread.sleep(100);
                        }
                        finally
                        {
                            lock.writeLock().release();
                        }
                    }
                }
                finally
                {
                    CloseableUtils.closeQuietly(client);
                }
                return null;
            }
        });

        writerThread.get();
        try
        {
            Assert.assertEquals(writerCount.get(), WRITER_ATTEMPTS);
        }
        finally
        {
            service.shutdownNow();
            for ( int i =0; i < CONCURRENCY; ++i )
            {
                completionService.take().get();
            }
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

        ExecutorService service = Executors.newCachedThreadPool();
        ExecutorCompletionService<Void> completionService = new ExecutorCompletionService<Void>(service);
        for ( int i = 0; i < CONCURRENCY; ++i )
        {
            completionService.submit(new Callable<Void>()
            {
                @Override
                public Void call() throws Exception
                {
                    CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
                    client.start();
                    try
                    {
                        InterProcessReadWriteLockBase lock = newLock(client, "/lock");
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
                        CloseableUtils.closeQuietly(client);
                    }
                    return null;
                }
            });
        }
        for ( int i =0; i < CONCURRENCY; ++i )
        {
            completionService.take().get();
        }

        System.out.println("Writes: " + writeCount.get() + " - Reads: " + readCount.get() + " - Max Reads: " + maxConcurrentCount.get());

        Assert.assertTrue(writeCount.get() > 0);
        Assert.assertTrue(readCount.get() > 0);
        Assert.assertTrue(maxConcurrentCount.get() > 1);
    }

    protected abstract InterProcessReadWriteLockBase newLock(CuratorFramework client, String path);

    private void doLocking(InterProcessLock lock, AtomicInteger concurrentCount, AtomicInteger maxConcurrentCount, Random random, int maxAllowed) throws Exception
    {
        boolean hasTheLock = false;
        try
        {
            Assert.assertTrue(lock.acquire(10, TimeUnit.SECONDS));
            hasTheLock = true;
            int localConcurrentCount;
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
            if ( hasTheLock )
            {
                synchronized(this)
                {
                    concurrentCount.decrementAndGet();
                    lock.release();
                }
            }
        }
    }
}
