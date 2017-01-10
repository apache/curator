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

package org.apache.curator.framework.recipes.shared;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
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
import java.util.concurrent.Phaser;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TestSharedCount extends BaseClassForTests
{
    @Test
    public void testMultiClients() throws Exception
    {
        final int CLIENT_QTY = 5;

        List<Future<List<Integer>>> futures = Lists.newArrayList();
        final List<CuratorFramework> clients = new CopyOnWriteArrayList<CuratorFramework>();
        final List<SharedCount> counts = new CopyOnWriteArrayList<SharedCount>();
        try
        {
            final CountDownLatch startLatch = new CountDownLatch(CLIENT_QTY);
            final Semaphore semaphore = new Semaphore(0);
            ExecutorService service = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("Test-%d").build());
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
                                counts.add(count);

                                final CountDownLatch latch = new CountDownLatch(1);
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

                                            @Override
                                            public void stateChanged(CuratorFramework client, ConnectionState newState)
                                            {
                                            }
                                        }
                                    );
                                count.start();
                                startLatch.countDown();
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

            Assert.assertTrue(startLatch.await(10, TimeUnit.SECONDS));

            SharedCount count = new SharedCount(client, "/count", 10);
            counts.add(count);
            count.start();

            List<Integer> countList = Lists.newArrayList();
            Random random = new Random();
            for ( int i = 0; i < 100; ++i )
            {
                Thread.sleep(random.nextInt(10));

                int next = random.nextInt(100);
                countList.add(next);
                count.setCount(next);

                Assert.assertTrue(semaphore.tryAcquire(CLIENT_QTY, 10, TimeUnit.SECONDS));
            }
            count.setCount(-1);

            for ( Future<List<Integer>> future : futures )
            {
                List<Integer> thisCountList = future.get();
                Assert.assertEquals(thisCountList, countList);
            }
        }
        finally
        {
            for ( SharedCount count : counts )
            {
                CloseableUtils.closeQuietly(count);
            }
            for ( CuratorFramework client : clients )
            {
                CloseableUtils.closeQuietly(client);
            }
        }
    }

    @Test
    public void testSimple() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        SharedCount count = new SharedCount(client, "/count", 0);
        try
        {
            client.start();
            count.start();

            Assert.assertTrue(count.trySetCount(1));
            Assert.assertTrue(count.trySetCount(2));
            Assert.assertTrue(count.trySetCount(10));
            Assert.assertEquals(count.getCount(), 10);
        }
        finally
        {
            CloseableUtils.closeQuietly(count);
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testSimpleVersioned() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        SharedCount count = new SharedCount(client, "/count", 0);
        client.start();
        try
        {
            count.start();

            VersionedValue<Integer> current = count.getVersionedValue();
            Assert.assertEquals(current.getVersion(), 0);

            Assert.assertTrue(count.trySetCount(current, 1));
            current = count.getVersionedValue();
            Assert.assertEquals(current.getVersion(), 1);
            Assert.assertEquals(count.getCount(), 1);

            Assert.assertTrue(count.trySetCount(current, 5));
            current = count.getVersionedValue();
            Assert.assertEquals(current.getVersion(), 2);
            Assert.assertEquals(count.getCount(), 5);

            Assert.assertTrue(count.trySetCount(current, 10));

            current = count.getVersionedValue();
            Assert.assertEquals(current.getVersion(), 3);
            Assert.assertEquals(count.getCount(), 10);

            // Wrong value
            Assert.assertFalse(count.trySetCount(new VersionedValue<Integer>(3, 20), 7));
            // Wrong version
            Assert.assertFalse(count.trySetCount(new VersionedValue<Integer>(10, 10), 7));

            // Server changed
            client.setData().forPath("/count", SharedCount.toBytes(88));
            Assert.assertFalse(count.trySetCount(current, 234));
        }
        finally
        {
            CloseableUtils.closeQuietly(count);
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testMultiClientVersioned() throws Exception
    {
        Timing timing = new Timing();
        CuratorFramework client1 = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        CuratorFramework client2 = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        SharedCount count1 = new SharedCount(client1, "/count", 0);
        SharedCount count2 = new SharedCount(client2, "/count", 0);
        try
        {
            client1.start();
            client2.start();
            count1.start();
            count2.start();

            VersionedValue<Integer> versionedValue = count1.getVersionedValue();
            Assert.assertTrue(count1.trySetCount(versionedValue, 10));
            timing.sleepABit();
            versionedValue = count2.getVersionedValue();
            Assert.assertTrue(count2.trySetCount(versionedValue, 20));
            timing.sleepABit();

            VersionedValue<Integer> versionedValue1 = count1.getVersionedValue();
            VersionedValue<Integer> versionedValue2 = count2.getVersionedValue();
            Assert.assertTrue(count2.trySetCount(versionedValue2, 30));
            Assert.assertFalse(count1.trySetCount(versionedValue1, 40));
            versionedValue1 = count1.getVersionedValue();
            Assert.assertTrue(count1.trySetCount(versionedValue1, 40));
        }
        finally
        {
            CloseableUtils.closeQuietly(count2);
            CloseableUtils.closeQuietly(count1);
            CloseableUtils.closeQuietly(client2);
            CloseableUtils.closeQuietly(client1);
        }
    }


    @Test
    public void testMultiClientDifferentSeed() throws Exception
    {
        CuratorFramework client1 = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        CuratorFramework client2 = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        SharedCount count1 = new SharedCount(client1, "/count", 10);
        SharedCount count2 = new SharedCount(client2, "/count", 20);
        try
        {
            client1.start();
            client2.start();
            count1.start();
            count2.start();

            Assert.assertEquals(count1.getCount(), 10);
            Assert.assertEquals(count2.getCount(), 10);
        }
        finally
        {
            CloseableUtils.closeQuietly(count2);
            CloseableUtils.closeQuietly(count1);
            CloseableUtils.closeQuietly(client2);
            CloseableUtils.closeQuietly(client1);
        }
    }


    @Test
    public void testDisconnectEventOnWatcherDoesNotRetry() throws Exception
    {
        final CountDownLatch gotSuspendEvent = new CountDownLatch(1);

        CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryNTimes(10, 1000));
        curatorFramework.start();
        curatorFramework.blockUntilConnected();

        SharedCount sharedCount = new SharedCount(curatorFramework, "/count", 10);
        sharedCount.start();

        curatorFramework.getConnectionStateListenable().addListener(new ConnectionStateListener() {
            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState) {
                if (newState == ConnectionState.SUSPENDED) {
                    gotSuspendEvent.countDown();
                }
            }
        });

        try
        {
            server.stop();
            // if watcher goes into 10second retry loop we won't get timely notification
            Assert.assertTrue(gotSuspendEvent.await(5, TimeUnit.SECONDS));
        }
        finally
        {
            CloseableUtils.closeQuietly(sharedCount);
            CloseableUtils.closeQuietly(curatorFramework);
        }
    }

    @Test
    public void testDisconnectReconnectEventDoesNotFireValueWatcher() throws Exception
    {
        final CountDownLatch gotSuspendEvent = new CountDownLatch(1);
        final CountDownLatch gotChangeEvent = new CountDownLatch(1);
        final CountDownLatch getReconnectEvent = new CountDownLatch(1);

        final AtomicInteger numChangeEvents = new AtomicInteger(0);


        CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryNTimes(10, 500));
        curatorFramework.start();
        curatorFramework.blockUntilConnected();

        SharedCount sharedCount = new SharedCount(curatorFramework, "/count", 10);

        sharedCount.addListener(new SharedCountListener() {
            @Override
            public void countHasChanged(SharedCountReader sharedCount, int newCount) throws Exception {
                numChangeEvents.incrementAndGet();
                gotChangeEvent.countDown();
            }

            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState) {
                if (newState == ConnectionState.SUSPENDED) {
                    gotSuspendEvent.countDown();
                } else if (newState == ConnectionState.RECONNECTED) {
                    getReconnectEvent.countDown();
                }
            }
        });
        sharedCount.start();

        try
        {
            sharedCount.setCount(11);
            Assert.assertTrue(gotChangeEvent.await(2, TimeUnit.SECONDS));

            server.stop();
            Assert.assertTrue(gotSuspendEvent.await(2, TimeUnit.SECONDS));

            server.restart();
            Assert.assertTrue(getReconnectEvent.await(2, TimeUnit.SECONDS));

            sharedCount.trySetCount(sharedCount.getVersionedValue(), 12);

            // flush background task queue
            final CountDownLatch flushDone = new CountDownLatch(1);
            curatorFramework.getData().inBackground(new BackgroundCallback() {
                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                    flushDone.countDown();
                }
            }).forPath("/count");
            flushDone.await(5, TimeUnit.SECONDS);

            Assert.assertEquals(2, numChangeEvents.get());
        }
        finally
        {
            CloseableUtils.closeQuietly(sharedCount);
            CloseableUtils.closeQuietly(curatorFramework);
        }
    }

    @Test
    public void testDisconnectReconnectEventDoesNotFireValueWatcherWithMultipleClients() throws Exception
    {
        CuratorFramework curatorFramework1 = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryNTimes(10, 500));
        CuratorFramework curatorFramework2 = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryNTimes(10, 500));
        curatorFramework1.start();
        curatorFramework1.blockUntilConnected();
        curatorFramework2.start();
        curatorFramework2.blockUntilConnected();

        SharedCount sharedCount1 = new SharedCount(curatorFramework1, "/count", 10);
        SharedCount sharedCount2 = new SharedCount(curatorFramework2, "/count", 10);

        class MySharedCountListener implements SharedCountListener
        {
            final public Phaser gotSuspendEvent = new Phaser(1);
            final public Phaser gotChangeEvent = new Phaser(1);
            final public Phaser getReconnectEvent = new Phaser(1);
            final public AtomicInteger numChangeEvents = new AtomicInteger(0);

            @Override
            public void countHasChanged(SharedCountReader sharedCount, int newCount) throws Exception
            {
                numChangeEvents.incrementAndGet();
                gotChangeEvent.arrive();
            }

            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState)
            {
                if (newState == ConnectionState.SUSPENDED) {
                    gotSuspendEvent.arrive();
                } else if (newState == ConnectionState.RECONNECTED) {
                    getReconnectEvent.arrive();
                }
            }
        }

        MySharedCountListener listener1 = new MySharedCountListener();
        sharedCount1.addListener(listener1);
        MySharedCountListener listener2 = new MySharedCountListener();
        sharedCount2.addListener(listener2);
        sharedCount1.start();
        sharedCount2.start();

        try
        {
            sharedCount1.setCount(11);
            Assert.assertEquals(listener1.gotChangeEvent.awaitAdvanceInterruptibly(0, 2, TimeUnit.SECONDS), 1);
            Assert.assertEquals(listener2.gotChangeEvent.awaitAdvanceInterruptibly(0, 2, TimeUnit.SECONDS), 1);
            Assert.assertEquals(sharedCount2.getCount(), 11);

            server.stop();
            Assert.assertEquals(listener1.gotSuspendEvent.awaitAdvanceInterruptibly(0, 2, TimeUnit.SECONDS), 1);
            Assert.assertEquals(listener2.gotSuspendEvent.awaitAdvanceInterruptibly(0, 2, TimeUnit.SECONDS), 1);

            server.restart();
            Assert.assertEquals(listener1.getReconnectEvent.awaitAdvanceInterruptibly(0, 2, TimeUnit.SECONDS), 1);
            Assert.assertEquals(listener2.getReconnectEvent.awaitAdvanceInterruptibly(0, 2, TimeUnit.SECONDS), 1);

            sharedCount1.trySetCount(sharedCount1.getVersionedValue(), 12);
            Assert.assertEquals(sharedCount1.getVersionedValue().getVersion(), 2);

            Assert.assertEquals(listener2.gotChangeEvent.awaitAdvanceInterruptibly(1, 2, TimeUnit.SECONDS), 2);
            Assert.assertEquals(sharedCount2.getCount(), 12);
            Assert.assertEquals(sharedCount2.getVersionedValue().getVersion(), 2);

            // flush background task queue
            final CountDownLatch flushDone = new CountDownLatch(1);
            curatorFramework1.getData().inBackground(new BackgroundCallback() {
                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                    flushDone.countDown();
                }
            }).forPath("/count");
            flushDone.await(5, TimeUnit.SECONDS);

            Assert.assertEquals(2, listener1.numChangeEvents.get());
            Assert.assertEquals(2, listener2.numChangeEvents.get());
        }
        finally
        {
            CloseableUtils.closeQuietly(sharedCount1);
            CloseableUtils.closeQuietly(curatorFramework1);
            CloseableUtils.closeQuietly(sharedCount2);
            CloseableUtils.closeQuietly(curatorFramework2);
        }
    }
}
