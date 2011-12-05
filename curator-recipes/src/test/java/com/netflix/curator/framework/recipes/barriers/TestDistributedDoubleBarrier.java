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

package com.netflix.curator.framework.recipes.barriers;

import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.recipes.BaseClassForTests;
import com.netflix.curator.retry.RetryOneTime;
import junit.framework.Assert;
import org.testng.annotations.Test;
import java.io.Closeable;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TestDistributedDoubleBarrier extends BaseClassForTests
{
    private static final int           QTY = 5;

    @Test
    public void     testMultiClient() throws Exception
    {
        final CountDownLatch    postEnterLatch = new CountDownLatch(QTY);
        final CountDownLatch    postLeaveLatch = new CountDownLatch(QTY);
        final AtomicInteger     count = new AtomicInteger(0);
        final AtomicInteger     max = new AtomicInteger(0);
        List<Future<Void>>      futures = Lists.newArrayList();
        ExecutorService         service = Executors.newCachedThreadPool();
        for ( int i = 0; i < QTY; ++i )
        {
            Future<Void>    future = service.submit
            (
                new Callable<Void>()
                {
                    @Override
                    public Void call() throws Exception
                    {
                        CuratorFramework                client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
                        try
                        {
                            client.start();
                            DistributedDoubleBarrier        barrier = new DistributedDoubleBarrier(client, "/barrier", QTY);

                            Assert.assertTrue(barrier.enter(10, TimeUnit.SECONDS));

                            synchronized(TestDistributedDoubleBarrier.this)
                            {
                                int     thisCount = count.incrementAndGet();
                                if ( thisCount > max.get() )
                                {
                                    max.set(thisCount);
                                }
                            }

                            postEnterLatch.countDown();
                            Assert.assertTrue(postEnterLatch.await(10, TimeUnit.SECONDS));

                            Assert.assertEquals(count.get(), QTY);

                            Assert.assertTrue(barrier.leave(10, TimeUnit.SECONDS));
                            count.decrementAndGet();

                            postLeaveLatch.countDown();
                            Assert.assertTrue(postLeaveLatch.await(10, TimeUnit.SECONDS));
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

        for ( Future<Void> f : futures )
        {
            f.get();
        }
        Assert.assertEquals(count.get(), 0);
        Assert.assertEquals(max.get(), QTY);
    }

    @Test
    public void     testBasic() throws Exception
    {
        final List<Closeable>     closeables = Lists.newArrayList();
        final CuratorFramework    client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            closeables.add(client);
            client.start();

            final CountDownLatch    postEnterLatch = new CountDownLatch(QTY);
            final CountDownLatch    postLeaveLatch = new CountDownLatch(QTY);
            final AtomicInteger     count = new AtomicInteger(0);
            final AtomicInteger     max = new AtomicInteger(0);
            List<Future<Void>>      futures = Lists.newArrayList();
            ExecutorService         service = Executors.newCachedThreadPool();
            for ( int i = 0; i < QTY; ++i )
            {
                Future<Void>    future = service.submit
                (
                    new Callable<Void>()
                    {
                        @Override
                        public Void call() throws Exception
                        {
                            DistributedDoubleBarrier        barrier = new DistributedDoubleBarrier(client, "/barrier", QTY);

                            Assert.assertTrue(barrier.enter(10, TimeUnit.SECONDS));

                            synchronized(TestDistributedDoubleBarrier.this)
                            {
                                int     thisCount = count.incrementAndGet();
                                if ( thisCount > max.get() )
                                {
                                    max.set(thisCount);
                                }
                            }

                            postEnterLatch.countDown();
                            Assert.assertTrue(postEnterLatch.await(10, TimeUnit.SECONDS));

                            Assert.assertEquals(count.get(), QTY);

                            Assert.assertTrue(barrier.leave(10, TimeUnit.SECONDS));
                            count.decrementAndGet();

                            postLeaveLatch.countDown();
                            Assert.assertTrue(postLeaveLatch.await(10, TimeUnit.SECONDS));

                            return null;
                        }
                    }
                );
                futures.add(future);
            }

            for ( Future<Void> f : futures )
            {
                f.get();
            }
            Assert.assertEquals(count.get(), 0);
            Assert.assertEquals(max.get(), QTY);
        }
        finally
        {
            for ( Closeable c : closeables )
            {
                Closeables.closeQuietly(c);
            }
        }
    }
}
