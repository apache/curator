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

import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.recipes.BaseClassForTests;
import com.netflix.curator.retry.RetryOneTime;
import junit.framework.Assert;
import org.testng.annotations.Test;
import org.testng.collections.Lists;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@SuppressWarnings({"SynchronizationOnLocalVariableOrMethodParameter"})
public class TestDistributedQueue extends BaseClassForTests
{
    private static final String     QUEUE_PATH = "/a/queue";

    private static final QueueSerializer<TestQueueItem>  serializer = new QueueItemSerializer();

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
                producerQueue = QueueBuilder.builder(producerClient, serializer, QUEUE_PATH).makeProducerOnly().buildQueue();
                producerQueue.start();
                QueueProducer       producer = new QueueProducer(producerQueue, itemQty, 0);
                service.submit(producer);
            }

            final Set<TestQueueItem>                takenItems = Sets.newTreeSet();
            final Set<TestQueueItem>                takenItemsForConsumer1 = Sets.newTreeSet();
            final Set<TestQueueItem>                takenItemsForConsumer2 = Sets.newTreeSet();
            final AtomicReference<TestQueueItem>    thrownItemFromConsumer1 = new AtomicReference<TestQueueItem>(null);

            // make the first consumer queue
            {
                final QueueSafetyConsumer<TestQueueItem>        ourQueue = new QueueSafetyConsumer<TestQueueItem>()
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
                consumerQueue1 = QueueBuilder.builder(consumerClient1, serializer, QUEUE_PATH).
                    queueSafety(new QueueSafety<TestQueueItem>("/a/locks", ourQueue)).
                    buildQueue();
                consumerQueue1.start();
            }

            // make the second consumer queue
            {
                final QueueSafetyConsumer<TestQueueItem>        ourQueue = new QueueSafetyConsumer<TestQueueItem>()
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
                consumerQueue2 = QueueBuilder.builder(consumerClient2, serializer, QUEUE_PATH).
                    queueSafety(new QueueSafety<TestQueueItem>("/a/locks", ourQueue)).
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
            final BlockingQueue<TestQueueItem>        ourQueue = new ArrayBlockingQueue<TestQueueItem>(1);
            queue = QueueBuilder.builder(client, serializer, QUEUE_PATH).
                queueSafety(new QueueSafety<TestQueueItem>("/a/locks", ourQueue)).
                buildQueue();
            queue.start();

            QueueProducer       producer = new QueueProducer(queue, itemQty, 0);

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
                            TestQueueItem item = ourQueue.take();
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
            queue = QueueBuilder.builder(client, serializer, QUEUE_PATH).buildQueue();
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
                TestQueueItem queueItem = queue.take(1, TimeUnit.SECONDS);
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
            queue = QueueBuilder.builder(client, serializer, QUEUE_PATH).buildQueue();
            queue.start();

            QueueProducer producer1 = new QueueProducer(queue, itemQty / 2, 0);
            QueueProducer       producer2 = new QueueProducer(queue, ((itemQty + 1) / 2), itemQty / 2);
            QueueConsumer consumer = new QueueConsumer(queue);

            ExecutorService     service = Executors.newCachedThreadPool();
            service.submit(producer1);
            service.submit(producer2);
            service.submit(consumer);

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
    public void     testSinglePutterMultiGetter() throws Exception
    {
        final int                   itemQty = 100;

        DistributedQueue<TestQueueItem>  queue = null;
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            queue = QueueBuilder.builder(client, serializer, QUEUE_PATH).buildQueue();
            queue.start();

            QueueProducer       producer = new QueueProducer(queue, itemQty, 0);
            QueueConsumer       consumer1 = new QueueConsumer(queue);
            QueueConsumer       consumer2 = new QueueConsumer(queue);

            ExecutorService     service = Executors.newCachedThreadPool();
            service.submit(producer);
            service.submit(consumer1);
            service.submit(consumer2);

            int                 iteration = 0;
            while ( (consumer1.size() + consumer2.size()) < itemQty )
            {
                Assert.assertTrue(++iteration < 10);
                Thread.sleep(1000);
            }

            List<TestQueueItem> items1 = consumer1.getItems();
            List<TestQueueItem> items2 = consumer2.getItems();
            Sets.SetView<TestQueueItem> overlap = Sets.intersection(Sets.<TestQueueItem>newHashSet(items1), Sets.<TestQueueItem>newHashSet(items2));
            Assert.assertEquals(overlap.size(), 0);

            Assert.assertEquals(Sets.<TestQueueItem>newHashSet(items1).size(), items1.size()); // check no duplicates
            Assert.assertEquals(Sets.<TestQueueItem>newHashSet(items2).size(), items2.size()); // check no duplicates

            List<TestQueueItem> items1Sorted = Lists.newArrayList(items1);
            Collections.sort(items1Sorted);
            Assert.assertEquals(items1Sorted, items1); // check ascending order

            List<TestQueueItem> items2Sorted = Lists.newArrayList(items2);
            Collections.sort(items2Sorted);
            Assert.assertEquals(items2Sorted, items2); // check ascending order
        }
        finally
        {
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
            queue = QueueBuilder.builder(client, serializer, QUEUE_PATH).buildQueue();
            queue.start();

            QueueProducer       producer = new QueueProducer(queue, itemQty, 0);
            QueueConsumer       consumer = new QueueConsumer(queue);

            ExecutorService     service = Executors.newCachedThreadPool();
            service.submit(producer);
            service.submit(consumer);

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
