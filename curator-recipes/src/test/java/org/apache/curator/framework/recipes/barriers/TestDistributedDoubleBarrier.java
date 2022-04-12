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
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.junit.jupiter.api.Test;

import java.io.Closeable;
import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class TestDistributedDoubleBarrier extends BaseClassForTests
{
    private static final int           QTY = 5;
    private static final int           ENTER_AFFECT_LEAVE_TEST_COUNT = 10;

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

                            assertTrue(barrier.enter(timing.seconds(), TimeUnit.SECONDS));

                            synchronized(TestDistributedDoubleBarrier.this)
                            {
                                int     thisCount = count.incrementAndGet();
                                if ( thisCount > max.get() )
                                {
                                    max.set(thisCount);
                                }
                            }

                            postEnterLatch.countDown();
                            assertTrue(timing.awaitLatch(postEnterLatch));

                            assertEquals(count.get(), QTY);

                            assertTrue(barrier.leave(timing.seconds(), TimeUnit.SECONDS));
                            count.decrementAndGet();

                            postLeaveLatch.countDown();
                            assertTrue(timing.awaitLatch(postEnterLatch));
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
        assertEquals(count.get(), 0);
        assertEquals(max.get(), QTY);
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
                                    assertTrue(timing.awaitLatch(latch));
                                    return super.getChildrenForEntering();
                                }
                            };
                            assertTrue(barrier.enter(timing.seconds(), TimeUnit.SECONDS));
                            assertTrue(barrier.leave(timing.seconds(), TimeUnit.SECONDS));
                            return null;
                        }
                    }
                );
            }

            assertTrue(semaphore.tryAcquire(QTY + 1, timing.seconds(), TimeUnit.SECONDS));   // wait until all QTY+1 barriers are trying to enter
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
    public void     testEnterAffectLeaveByWatcher() throws Exception
    {
        for (int count = 0; count < ENTER_AFFECT_LEAVE_TEST_COUNT; count++) {
            final Timing            timing = new Timing();
            List<Future<Void>>      futures = Lists.newArrayList();
            CountDownLatch          leaveLatch = new CountDownLatch(1);
            ExecutorService         service = Executors.newCachedThreadPool();

            for ( int i = 0; i < QTY-1; ++i )
            {
                final int       index = i;
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

                                            assertTrue(barrier.enter(timing.seconds(), TimeUnit.SECONDS));

                                            assertTrue(barrier.leave(timing.seconds(), TimeUnit.SECONDS));

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


            CuratorFramework                client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
            client.start();
            DistributedDoubleBarrier        barrier = new DistributedDoubleBarrier(client, "/barrier", QTY);
            barrier.enter();

            AtomicBoolean error = new AtomicBoolean(true);

            Future<Void> leaveFuture = service.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    Field ourPathField = barrier.getClass().getDeclaredField("ourPath");
                    ourPathField.setAccessible(true);
                    String ourPath = (String) ourPathField.get(barrier);
                    leaveLatch.await(timing.seconds()/2,TimeUnit.SECONDS);
                    client.delete().forPath(ourPath);
                    error.set(false);
                    return null;
                }
            });

            for ( Future<Void> f : futures )
            {
                f.get();
            }

            assertFalse(error.get());
            leaveLatch.countDown();
            leaveFuture.get();
            List<String> children = client.getChildren().forPath("/barrier");
            assertTrue(children.isEmpty());
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

                            assertTrue(barrier.enter(timing.seconds(), TimeUnit.SECONDS));

                            synchronized(TestDistributedDoubleBarrier.this)
                            {
                                int     thisCount = count.incrementAndGet();
                                if ( thisCount > max.get() )
                                {
                                    max.set(thisCount);
                                }
                            }

                            postEnterLatch.countDown();
                            assertTrue(timing.awaitLatch(postEnterLatch));

                            assertEquals(count.get(), QTY);

                            assertTrue(barrier.leave(10, TimeUnit.SECONDS));
                            count.decrementAndGet();

                            postLeaveLatch.countDown();
                            assertTrue(timing.awaitLatch(postLeaveLatch));

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
            assertEquals(count.get(), 0);
            assertEquals(max.get(), QTY);
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
