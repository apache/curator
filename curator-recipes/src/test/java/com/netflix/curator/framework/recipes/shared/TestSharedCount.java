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

package com.netflix.curator.framework.recipes.shared;

import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.recipes.BaseClassForTests;
import com.netflix.curator.retry.RetryOneTime;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class TestSharedCount extends BaseClassForTests
{
    @Test
    public void     testMultiClients() throws Exception
    {
        final int           CLIENT_QTY = 5;

        List<Future<List<Integer>>>     futures = Lists.newArrayList();
        final List<CuratorFramework>    clients = new CopyOnWriteArrayList<CuratorFramework>();
        try
        {
            final Semaphore     semaphore = new Semaphore(0);
            ExecutorService     service = Executors.newCachedThreadPool();
            for ( int i = 0; i < CLIENT_QTY; ++i )
            {
                Future<List<Integer>> future = service.submit
                (
                    new Callable<List<Integer>>()
                    {
                        @Override
                        public List<Integer> call() throws Exception
                        {
                            final List<Integer> countList = Lists.newArrayList();
                            CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
                            clients.add(client);
                            client.start();

                            SharedCount count = new SharedCount(client, "/count", 10);
                            count.start();

                            final CountDownLatch        latch = new CountDownLatch(1);
                            count.addListener
                            (
                                new SharedCountListener()
                                {
                                    @Override
                                    public void countHasChanged(SharedCountReader sharedCount, int newCount) throws Exception
                                    {
                                        if ( newCount < 0 )
                                        {
                                            latch.countDown();
                                        }
                                        else
                                        {
                                            countList.add(newCount);
                                        }

                                        semaphore.release();
                                    }
                                }
                            );
                            latch.await();
                            return countList;
                        }
                    }
                );
                futures.add(future);
            }

            CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
            clients.add(client);
            client.start();

            SharedCount count = new SharedCount(client, "/count", 10);
            count.start();

            List<Integer> countList = Lists.newArrayList();
            Random        random = new Random();
            for ( int i = 0; i < 100; ++i )
            {
                Thread.sleep(random.nextInt(10));

                int     next = random.nextInt(100);
                countList.add(next);
                count.setCount(next);

                Assert.assertTrue(semaphore.tryAcquire(CLIENT_QTY, 10, TimeUnit.SECONDS));
            }
            count.setCount(-1);

            for ( Future<List<Integer>> future : futures )
            {
                List<Integer>       thisCountList = future.get();
                Assert.assertEquals(thisCountList, countList);
            }
        }
        finally
        {
            for ( CuratorFramework client : clients )
            {
                Closeables.closeQuietly(client);
            }
        }
    }

    @Test
    public void     testSimple() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            SharedCount         count = new SharedCount(client, "/count", 0);
            count.start();

            Assert.assertTrue(count.trySetCount(1));
            Assert.assertTrue(count.trySetCount(2));
            Assert.assertTrue(count.trySetCount(10));
            Assert.assertEquals(count.getCount(), 10);
        }
        finally
        {
            client.close();
        }
    }
}
