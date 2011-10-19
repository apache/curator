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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@SuppressWarnings({"SynchronizationOnLocalVariableOrMethodParameter"})
public class TestInterProcessSemaphore extends BaseClassForTests
{
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
        final int       MAX_LEASES = 10;
        final int       THREADS = 100;

        final CuratorFramework    client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            final CountDownLatch  latch = new CountDownLatch(THREADS);
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

            for ( int i = 0; i < THREADS; ++i )
            {
                Thread.sleep(random.nextInt(10) + 1);
                latch.countDown();
            }
            Thread.sleep(1000);

            synchronized(counter)
            {
                Assert.assertTrue(counter.currentCount == 0);
                Assert.assertEquals(counter.maxCount, MAX_LEASES);
            }
        }
        finally
        {
            client.close();
        }
    }

    @Test
    public void     testSimple() throws Exception
    {
        final int       MAX_LEASES = 3;

        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            List<InterProcessSemaphore>        leases = Lists.newArrayList();
            for ( int i = 0; i < MAX_LEASES; ++i )
            {
                InterProcessSemaphore      semaphore = new InterProcessSemaphore(client, "/test", MAX_LEASES);
                Assert.assertTrue(semaphore.acquire(10, TimeUnit.SECONDS));
                leases.add(semaphore);
            }

            InterProcessSemaphore      semaphore = new InterProcessSemaphore(client, "/test", MAX_LEASES);
            Assert.assertFalse(semaphore.acquire(3, TimeUnit.SECONDS));

            leases.remove(0).release();
            Assert.assertTrue(semaphore.acquire(10, TimeUnit.SECONDS));
        }
        finally
        {
            client.close();
        }
    }
}
