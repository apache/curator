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
package com.netflix.curator.framework.recipes.queue;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.api.CuratorEvent;
import com.netflix.curator.framework.api.CuratorListener;
import com.netflix.curator.framework.recipes.BaseClassForTests;
import com.netflix.curator.retry.RetryOneTime;
import junit.framework.Assert;
import org.testng.annotations.Test;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@SuppressWarnings({"SynchronizationOnLocalVariableOrMethodParameter"})
public class TestDistributedQueue extends BaseClassForTests
{
    private static final String     QUEUE_PATH = "/a/queue";

    private static final QueueSerializer<TestQueueItem>  serializer = new QueueItemSerializer();

    @Test
    public void     testErrorMode() throws Exception
    {
        CuratorFramework          client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            final AtomicReference<CountDownLatch>   latch = new AtomicReference<CountDownLatch>(new CountDownLatch(1));
            final AtomicInteger                     count = new AtomicInteger(0);
            QueueConsumer<TestQueueItem>            consumer = new QueueConsumer<TestQueueItem>()
            {
                @Override
                public void consumeMessage(TestQueueItem message) throws Exception
                {
                    if ( count.incrementAndGet() < 2 )
                    {
                        throw new Exception();
                    }
                    latch.get().countDown();
                }
            };
            DistributedQueue<TestQueueItem> queue = QueueBuilder.builder(client, consumer, serializer, QUEUE_PATH).lockPath("/locks").buildQueue();
            try
            {
                queue.start();

                TestQueueItem       item = new TestQueueItem("1");
                queue.put(item);

                Assert.assertTrue(latch.get().await(10, TimeUnit.SECONDS));
                Assert.assertEquals(count.get(), 2);

                queue.setErrorMode(ErrorMode.DELETE);

                count.set(0);
                latch.set(new CountDownLatch(1));

                item = new TestQueueItem("1");
                queue.put(item);

                Assert.assertFalse(latch.get().await(5, TimeUnit.SECONDS)); // consumer should get called only once
                Assert.assertEquals(count.get(), 1);
            }
            finally
            {
                queue.close();
            }
        }
        finally
        {
            client.close();
        }
    }

    @Test
    public void     testNoDuplicateProcessing() throws Exception
    {
        final int                 itemQty = 1000;
        final int                 consumerQty = 4;

        CuratorFramework          client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            DistributedQueue<TestQueueItem> producerQueue = QueueBuilder.builder(client, null, serializer, QUEUE_PATH).buildQueue();
            try
            {
                producerQueue.start();
                for ( int i = 0; i < itemQty; ++i )
                {
                    TestQueueItem       item = new TestQueueItem(Integer.toString(i));
                    producerQueue.put(item);
                }
                producerQueue.flushPuts(10, TimeUnit.SECONDS);
            }
            finally
            {
                producerQueue.close();
            }
        }
        finally
        {
            client.close();
        }

        final Set<String>                consumedMessages = Sets.newHashSet();
        final Set<String>                duplicateMessages = Sets.newHashSet();

        final CountDownLatch             latch = new CountDownLatch(itemQty);
        List<DistributedQueue<TestQueueItem>>   consumers = Lists.newArrayList();
        List<CuratorFramework>                  consumerClients = Lists.newArrayList();
        try
        {
            final QueueConsumer<TestQueueItem> ourQueue = new QueueConsumer<TestQueueItem>()
            {
                @Override
                public void consumeMessage(TestQueueItem message)
                {
                    synchronized(consumedMessages)
                    {
                        if ( consumedMessages.contains(message.str) )
                        {
                            duplicateMessages.add(message.str);
                        }
                        consumedMessages.add(message.str);
                    }
                    latch.countDown();
                }
            };

            for ( int i = 0; i < consumerQty; ++i )
            {
                CuratorFramework thisClient = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
                consumerClients.add(thisClient);
                thisClient.start();

                DistributedQueue<TestQueueItem>     thisConsumer = QueueBuilder.builder(thisClient, ourQueue, serializer, QUEUE_PATH).
                    lockPath("/a/locks").
                    buildQueue();
                consumers.add(thisConsumer);
            }
            for ( DistributedQueue<TestQueueItem> consumer : consumers )
            {
                consumer.start();
            }

            Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
            Assert.assertTrue(duplicateMessages.toString(), duplicateMessages.size() == 0);
        }
        finally
        {
            for ( DistributedQueue<TestQueueItem> consumer : consumers )
            {
                 Closeables.closeQuietly(consumer);
            }
            for ( CuratorFramework curatorFramework : consumerClients )
            {
                Closeables.closeQuietly(curatorFramework);
            }
        }
    }

    @Test
    public void     testSafetyWithCrash() throws Exception
    {
        final int                   itemQty = 100;

        DistributedQueue<TestQueueItem>  producerQueue = null;
        DistributedQueue<TestQueueItem>  consumerQueue1 = null;
        DistributedQueue<TestQueueItem>  consumerQueue2 = null;

        CuratorFramework                 producerClient = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        CuratorFramework                 consumerClient1 = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        CuratorFramework                 consumerClient2 = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            producerClient.start();
            consumerClient1.start();
            consumerClient2.start();

            ExecutorService     service = Executors.newCachedThreadPool();

            // make the producer queue
            {
                producerQueue = QueueBuilder.builder(producerClient, null, serializer, QUEUE_PATH).buildQueue();
                producerQueue.start();
                QueueTestProducer producer = new QueueTestProducer(producerQueue, itemQty, 0);
                service.submit(producer);
            }

            final Set<TestQueueItem>                takenItems = Sets.newTreeSet();
            final Set<TestQueueItem>                takenItemsForConsumer1 = Sets.newTreeSet();
            final Set<TestQueueItem>                takenItemsForConsumer2 = Sets.newTreeSet();
            final AtomicReference<TestQueueItem>    thrownItemFromConsumer1 = new AtomicReference<TestQueueItem>(null);

            // make the first consumer queue
            {
                final QueueConsumer<TestQueueItem> ourQueue = new QueueConsumer<TestQueueItem>()
                {
                    @Override
                    public void consumeMessage(TestQueueItem message) throws Exception
                    {
                        synchronized(takenItems)
                        {
                            if ( takenItems.size() > 10 )
                            {
                                thrownItemFromConsumer1.set(message);
                                throw new Exception("dummy");   // simulate a crash
                            }
                        }
                        
                        addToTakenItems(message, takenItems, itemQty);
                        synchronized(takenItemsForConsumer1)
                        {
                            takenItemsForConsumer1.add(message);
                        }

                        Thread.sleep((long)(Math.random() * 5));
                    }
                };
                consumerQueue1 = QueueBuilder.builder(consumerClient1, ourQueue, serializer, QUEUE_PATH).
                    lockPath("/a/locks").
                    buildQueue();
                consumerQueue1.start();
            }

            // make the second consumer queue
            {
                final QueueConsumer<TestQueueItem> ourQueue = new QueueConsumer<TestQueueItem>()
                {
                    @Override
                    public void consumeMessage(TestQueueItem message) throws Exception
                    {
                        addToTakenItems(message, takenItems, itemQty);
                        synchronized(takenItemsForConsumer2)
                        {
                            takenItemsForConsumer2.add(message);
                        }
                        Thread.sleep((long)(Math.random() * 5));
                    }
                };
                consumerQueue2 = QueueBuilder.builder(consumerClient2, ourQueue, serializer, QUEUE_PATH).
                    lockPath("/a/locks").
                    buildQueue();
                consumerQueue2.start();
            }

            synchronized(takenItems)
            {
                while ( takenItems.size() < itemQty )
                {
                    takenItems.wait(1000);
                }
            }

            int                 i = 0;
            for ( TestQueueItem item : takenItems )
            {
                Assert.assertEquals(item.str, Integer.toString(i++));
            }

            Assert.assertNotNull(thrownItemFromConsumer1.get());
            Assert.assertTrue((takenItemsForConsumer2.contains(thrownItemFromConsumer1.get())));
            Assert.assertTrue(Sets.intersection(takenItemsForConsumer1, takenItemsForConsumer2).size() == 0);
        }
        finally
        {
            Closeables.closeQuietly(producerQueue);
            Closeables.closeQuietly(consumerQueue1);
            Closeables.closeQuietly(consumerQueue2);

            Closeables.closeQuietly(producerClient);
            Closeables.closeQuietly(consumerClient1);
            Closeables.closeQuietly(consumerClient2);
        }
    }

    private void addToTakenItems(TestQueueItem message, Set<TestQueueItem> takenItems, int itemQty)
    {
        synchronized(takenItems)
        {
            takenItems.add(message);
            if ( takenItems.size() > itemQty )
            {
                takenItems.notifyAll();
            }
        }
    }

    @Test
    public void     testSafetyBasic() throws Exception
    {
        final int                   itemQty = 10;

        DistributedQueue<TestQueueItem>  queue = null;
        CuratorFramework                 client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            final BlockingQueueConsumer<TestQueueItem> consumer = new BlockingQueueConsumer<TestQueueItem>();
            queue = QueueBuilder.builder(client, consumer, serializer, QUEUE_PATH).
                lockPath("/a/locks").
                buildQueue();
            queue.start();

            QueueTestProducer producer = new QueueTestProducer(queue, itemQty, 0);

            ExecutorService     service = Executors.newCachedThreadPool();
            service.submit(producer);

            final CountDownLatch        latch = new CountDownLatch(1);
            service.submit
            (
                new Callable<Object>()
                {
                    @Override
                    public Object call() throws Exception
                    {
                        for ( int i = 0; i < itemQty; ++i )
                        {
                            TestQueueItem item = consumer.take();
                            Assert.assertEquals(item.str, Integer.toString(i));
                        }
                        latch.countDown();
                        return null;
                    }
                }
            );
            Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
        }
        finally
        {
            Closeables.closeQuietly(queue);
            Closeables.closeQuietly(client);
        }
    }

    @Test
    public void     testPutMulti() throws Exception
    {
        final int                   itemQty = 100;

        DistributedQueue<TestQueueItem>  queue = null;
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            BlockingQueueConsumer<TestQueueItem> consumer = new BlockingQueueConsumer<TestQueueItem>();

            queue = QueueBuilder.builder(client, consumer, serializer, QUEUE_PATH).buildQueue();
            queue.start();

            MultiItem<TestQueueItem>    items = new MultiItem<TestQueueItem>()
            {
                private int     index = 0;

                @Override
                public TestQueueItem nextItem() throws Exception
                {
                    if ( index >= itemQty )
                    {
                        return null;
                    }
                    return new TestQueueItem(Integer.toString(index++));
                }
            };
            queue.putMulti(items);

            for ( int i = 0; i < itemQty; ++i )
            {
                TestQueueItem   queueItem = consumer.take(1, TimeUnit.SECONDS);
                Assert.assertNotNull(queueItem);
                Assert.assertEquals(queueItem, new TestQueueItem(Integer.toString(i)));
            }
        }
        finally
        {
            Closeables.closeQuietly(queue);
            Closeables.closeQuietly(client);
        }
    }

    @Test
    public void     testMultiPutterSingleGetter() throws Exception
    {
        final int                   itemQty = 100;

        DistributedQueue<TestQueueItem>  queue = null;
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            BlockingQueueConsumer<TestQueueItem> consumer = new BlockingQueueConsumer<TestQueueItem>();

            queue = QueueBuilder.builder(client, consumer, serializer, QUEUE_PATH).buildQueue();
            queue.start();

            QueueTestProducer producer1 = new QueueTestProducer(queue, itemQty / 2, 0);
            QueueTestProducer producer2 = new QueueTestProducer(queue, ((itemQty + 1) / 2), itemQty / 2);

            ExecutorService     service = Executors.newCachedThreadPool();
            service.submit(producer1);
            service.submit(producer2);

            int                 iteration = 0;
            while ( consumer.size() < itemQty )
            {
                Assert.assertTrue(++iteration < 10);
                Thread.sleep(1000);
            }

            List<TestQueueItem> items = consumer.getItems();

            Assert.assertEquals(com.google.common.collect.Sets.<TestQueueItem>newHashSet(items).size(), items.size()); // check no duplicates
        }
        finally
        {
            Closeables.closeQuietly(queue);
            Closeables.closeQuietly(client);
        }
    }

    @Test
    public void     testFlush() throws Exception
    {
        final CountDownLatch              latch = new CountDownLatch(1);
        DistributedQueue<TestQueueItem>   queue = null;
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            client.addListener
            (
                new CuratorListener()
                {
                    @Override
                    public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception
                    {
                        // this listener should get called before the queue's listener
                        latch.await();
                    }

                    @Override
                    public void unhandledError(CuratorFramework client, Throwable e)
                    {
                    }
                }
            );

            queue = QueueBuilder.builder(client, null, serializer, "/test").buildQueue();
            queue.start();

            queue.put(new TestQueueItem("1"));
            Assert.assertFalse(queue.flushPuts(3, TimeUnit.SECONDS));
            latch.countDown();

            Assert.assertTrue(queue.flushPuts(10, TimeUnit.SECONDS));
        }
        finally
        {
            if ( latch.getCount() > 0 )
            {
                latch.countDown();
            }

            Closeables.closeQuietly(queue);
            Closeables.closeQuietly(client);
        }
    }

    @Test
    public void     testSimple() throws Exception
    {
        final int                   itemQty = 10;

        DistributedQueue<TestQueueItem>  queue = null;
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            BlockingQueueConsumer<TestQueueItem> consumer = new BlockingQueueConsumer<TestQueueItem>();

            queue = QueueBuilder.builder(client, consumer, serializer, QUEUE_PATH).buildQueue();
            queue.start();

            QueueTestProducer producer = new QueueTestProducer(queue, itemQty, 0);

            ExecutorService     service = Executors.newCachedThreadPool();
            service.submit(producer);

            int                 iteration = 0;
            while ( consumer.size() < itemQty )
            {
                Assert.assertTrue(++iteration < 10);
                Thread.sleep(1000);
            }

            int                 i = 0;
            for ( TestQueueItem item : consumer.getItems() )
            {
                Assert.assertEquals(item.str, Integer.toString(i++));
            }
        }
        finally
        {
            Closeables.closeQuietly(queue);
            Closeables.closeQuietly(client);
        }
    }

}
