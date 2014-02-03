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
package org.apache.curator.framework.recipes.queue;

import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.BaseClassForTests;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.Timing;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

public class TestDistributedPriorityQueue extends BaseClassForTests
{
    @Test
    public void     testMinItemsBeforeRefresh() throws Exception
    {
        DistributedPriorityQueue<Integer>   queue = null;
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            final int minItemsBeforeRefresh = 3;

            BlockingQueueConsumer<Integer> consumer = new BlockingQueueConsumer<Integer>(Mockito.mock(ConnectionStateListener.class));
            queue = QueueBuilder.builder(client, consumer, new IntSerializer(), "/test").buildPriorityQueue(minItemsBeforeRefresh);
            queue.start();

            for ( int i = 0; i < 10; ++i )
            {
                queue.put(i, 10 + i);
            }

            Assert.assertEquals(consumer.take(1, TimeUnit.SECONDS), new Integer(0));
            queue.put(1000, 1); // lower priority

            int         count = 0;
            while ( consumer.take(1, TimeUnit.SECONDS) < 1000 )
            {
                ++count;
            }
            Assert.assertTrue(Math.abs(minItemsBeforeRefresh - count) < minItemsBeforeRefresh, String.format("Diff: %d - min: %d", Math.abs(minItemsBeforeRefresh - count), minItemsBeforeRefresh));     // allows for some slack - testing that within a slop value the newly inserted item with lower priority comes out
        }
        finally
        {
            CloseableUtils.closeQuietly(queue);
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void     testSortingWhileTaking() throws Exception
    {
        Timing           timing = new Timing();
        DistributedPriorityQueue<Integer>   queue = null;
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        client.start();
        try
        {
            final BlockingQueue<Integer>    blockingQueue = new SynchronousQueue<Integer>();
            QueueConsumer<Integer>          consumer = new QueueConsumer<Integer>()
            {
                @Override
                public void consumeMessage(Integer message) throws Exception
                {
                    blockingQueue.put(message);
                }

                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState)
                {
                }
            };
            queue = QueueBuilder.builder(client, consumer, new IntSerializer(), "/test").buildPriorityQueue(0);
            queue.start();

            for ( int i = 0; i < 10; ++i )
            {
                queue.put(i, 10);
            }

            Assert.assertEquals(blockingQueue.poll(timing.seconds(), TimeUnit.SECONDS), new Integer(0));
            timing.sleepABit();
            queue.put(1000, 1); // lower priority
            timing.sleepABit();
            Assert.assertEquals(blockingQueue.poll(timing.seconds(), TimeUnit.SECONDS), new Integer(1)); // is in consumer already
            Assert.assertEquals(blockingQueue.poll(timing.seconds(), TimeUnit.SECONDS), new Integer(1000));
        }
        finally
        {
            CloseableUtils.closeQuietly(queue);
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void     testAdditions() throws Exception
    {
        DistributedPriorityQueue<Integer>   queue = null;
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            final CountDownLatch        latch = new CountDownLatch(1);
            QueueSerializer<Integer>    serializer = new IntSerializer()
            {
                @Override
                public Integer deserialize(byte[] bytes)
                {
                    // gets called in the Queue's event processing thread
                    try
                    {
                        latch.await();
                    }
                    catch ( InterruptedException e )
                    {
                        // ignore
                    }
                    return super.deserialize(bytes);
                }
            };
            BlockingQueueConsumer<Integer> consumer = new BlockingQueueConsumer<Integer>(Mockito.mock(ConnectionStateListener.class));
            queue = QueueBuilder.builder(client, consumer, serializer, "/test").buildPriorityQueue(1);
            queue.start();

            for ( int i = 0; i < 10; ++i )
            {
                queue.put(10, 10);
                if ( i == 0 )
                {
                    queue.put(1, 1);
                    latch.countDown();
                }
            }

            assertOrdering(consumer, 10);
        }
        finally
        {
            CloseableUtils.closeQuietly(queue);
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void     testSimple() throws Exception
    {
        List<Integer>                       nums = new ArrayList<Integer>();

        Timing                              timing = new Timing();
        DistributedPriorityQueue<Integer>   queue = null;
        CuratorFramework                    client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        client.start();
        try
        {
            final CountDownLatch            hasConsumedLatch = new CountDownLatch(1);
            final CountDownLatch            okToConsumeLatch = new CountDownLatch(1);
            BlockingQueueConsumer<Integer>  consumer = new BlockingQueueConsumer<Integer>(Mockito.mock(ConnectionStateListener.class))
            {
                @Override
                public void consumeMessage(Integer message) throws Exception
                {
                    hasConsumedLatch.countDown();
                    okToConsumeLatch.await();
                    super.consumeMessage(message);
                }
            };
            queue = QueueBuilder.builder(client, consumer, new IntSerializer(), "/test").buildPriorityQueue(0);
            queue.start();

            nums.add(Integer.MIN_VALUE);
            queue.put(Integer.MIN_VALUE, Integer.MIN_VALUE);  // the queue background thread will be blocking with the first item - make sure it's the lowest value
            Assert.assertTrue(timing.awaitLatch(hasConsumedLatch));

            Random          random = new Random();
            for ( int i = 0; i < 100; ++i )
            {
                int     priority = random.nextInt();
                nums.add(priority);
                queue.put(priority, priority);
            }

            while ( queue.getCache().getData().children.size() < (nums.size() - 1) )    // -1 because the first message has already been consumed
            {
                timing.sleepABit(); // wait for the cache to catch up
            }
            okToConsumeLatch.countDown();

            assertOrdering(consumer, nums.size());
        }
        catch ( AssertionError e )
        {
            StringBuilder   message = new StringBuilder(e.getMessage());
            for ( int i : nums )
            {
                message.append(i).append("\t").append(DistributedPriorityQueue.priorityToString(i)).append("\n");
            }
            Assert.fail(message.toString());
        }
        finally
        {
            CloseableUtils.closeQuietly(queue);
            CloseableUtils.closeQuietly(client);
        }
    }

    private void assertOrdering(BlockingQueueConsumer<Integer> consumer, int qty) throws Exception
    {
        int         previous = 0;
        for ( int i = 0; i < qty; ++i )
        {
            Integer     value = consumer.take(10, TimeUnit.SECONDS);
            Assert.assertNotNull(value);
            if ( i > 0 )
            {
                Assert.assertTrue(value >= previous, String.format("Value: (%d:%s) Previous: (%d:%s)", value, DistributedPriorityQueue.priorityToString(value), previous, DistributedPriorityQueue.priorityToString(previous)));
            }
            previous = value;
        }
    }

    private static class IntSerializer implements QueueSerializer<Integer>
    {
        @Override
        public byte[] serialize(Integer item)
        {
            return Integer.toString(item).getBytes();
        }

        @Override
        public Integer deserialize(byte[] bytes)
        {
            return Integer.parseInt(new String(bytes));
        }
    }
}
