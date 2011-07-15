/*
 Copyright 2011 Netflix, Inc.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TestDistributedQueue extends BaseClassForTests
{
    private static final String     QUEUE_PATH = "/a/queue";

    private static final QueueSerializer<TestQueueItem>  serializer = new QueueItemSerializer();

    @Test
    public void     testMultiPutterSingleGetter() throws Exception
    {
        final int                   itemQty = 100;

        DistributedQueue<TestQueueItem>  queue = null;
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            queue = new DistributedQueue<TestQueueItem>(client, serializer, QUEUE_PATH);
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
            queue = new DistributedQueue<TestQueueItem>(client, serializer, QUEUE_PATH);
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
            queue = new DistributedQueue<TestQueueItem>(client, serializer, QUEUE_PATH);
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
