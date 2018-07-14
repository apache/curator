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
package org.apache.curator.framework.recipes.barriers;

import com.google.common.collect.Lists;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.Timing;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.io.Closeable;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import static org.testng.Assert.assertFalse;

public class TestDistributedDoubleBarrier extends BaseClassForTests
{
    private static final int           QTY = 5;

    @Test
    public void testMultipleBarrierConnectWithTimeout() throws Exception{
        // the issue is that after the first clients attempt to enter the barrier returns false,
        // that client has been told that it is not in the barrier. But when the second call is made (by client 2),
        // client 2 perceives client 1 as being in the barrier, so it returns true despite there really only being
        // one client at the barrier
        CuratorFramework client1 = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        CuratorFramework client2 = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client1.start();
        client2.start();
        DistributedDoubleBarrier barrierForClient1 = new DistributedDoubleBarrier(client1,"/barrier",2);
        DistributedDoubleBarrier barrierForClient2 = new DistributedDoubleBarrier(client2,"/barrier",2);
        assertFalse(barrierForClient1.enter(2, TimeUnit.SECONDS));
        assertFalse(barrierForClient2.enter(2,TimeUnit.SECONDS));
    }

    @Test
    public void     testMultiClient() throws Exception
    {
        final Timing            timing = new Timing();
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
                        CuratorFramework                client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
                        try
                        {
                            client.start();
                            DistributedDoubleBarrier        barrier = new DistributedDoubleBarrier(client, "/barrier", QTY);

                            Assert.assertTrue(barrier.enter(timing.seconds(), TimeUnit.SECONDS));

                            synchronized(TestDistributedDoubleBarrier.this)
                            {
                                int     thisCount = count.incrementAndGet();
                                if ( thisCount > max.get() )
                                {
                                    max.set(thisCount);
                                }
                            }

                            postEnterLatch.countDown();
                            Assert.assertTrue(timing.awaitLatch(postEnterLatch));

                            Assert.assertEquals(count.get(), QTY);

                            Assert.assertTrue(barrier.leave(timing.seconds(), TimeUnit.SECONDS));
                            count.decrementAndGet();

                            postLeaveLatch.countDown();
                            Assert.assertTrue(timing.awaitLatch(postEnterLatch));
                        }
                        finally
                        {
                            CloseableUtils.closeQuietly(client);
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
    public void     testOverSubscribed() throws Exception
    {
        final Timing                    timing = new Timing();
        final CuratorFramework          client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        ExecutorService                 service = Executors.newCachedThreadPool();
        ExecutorCompletionService<Void> completionService = new ExecutorCompletionService<Void>(service);
        try
        {
            client.start();

            final Semaphore         semaphore = new Semaphore(0);
            final CountDownLatch    latch = new CountDownLatch(1);
            for ( int i = 0; i < (QTY + 1); ++i )
            {
                completionService.submit
                (
                    new Callable<Void>()
                    {
                        @Override
                        public Void call() throws Exception
                        {
                            DistributedDoubleBarrier        barrier = new DistributedDoubleBarrier(client, "/barrier", QTY)
                            {
                                @Override
                                protected List<String> getChildrenForEntering() throws Exception
                                {
                                    semaphore.release();
                                    Assert.assertTrue(timing.awaitLatch(latch));
                                    return super.getChildrenForEntering();
                                }
                            };
                            Assert.assertTrue(barrier.enter(timing.seconds(), TimeUnit.SECONDS));
                            Assert.assertTrue(barrier.leave(timing.seconds(), TimeUnit.SECONDS));
                            return null;
                        }
                    }
                );
            }

            Assert.assertTrue(semaphore.tryAcquire(QTY + 1, timing.seconds(), TimeUnit.SECONDS));   // wait until all QTY+1 barriers are trying to enter
            latch.countDown();

            for ( int i = 0; i < (QTY + 1); ++i )
            {
                completionService.take().get(); // to check for assertions
            }
        }
        finally
        {
            service.shutdown();
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void     testBasic() throws Exception
    {
        final Timing              timing = new Timing();
        final List<Closeable>     closeables = Lists.newArrayList();
        final CuratorFramework    client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
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

                            Assert.assertTrue(barrier.enter(timing.seconds(), TimeUnit.SECONDS));

                            synchronized(TestDistributedDoubleBarrier.this)
                            {
                                int     thisCount = count.incrementAndGet();
                                if ( thisCount > max.get() )
                                {
                                    max.set(thisCount);
                                }
                            }

                            postEnterLatch.countDown();
                            Assert.assertTrue(timing.awaitLatch(postEnterLatch));

                            Assert.assertEquals(count.get(), QTY);

                            Assert.assertTrue(barrier.leave(10, TimeUnit.SECONDS));
                            count.decrementAndGet();

                            postLeaveLatch.countDown();
                            Assert.assertTrue(timing.awaitLatch(postLeaveLatch));

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
                CloseableUtils.closeQuietly(c);
            }
        }
    }
}
