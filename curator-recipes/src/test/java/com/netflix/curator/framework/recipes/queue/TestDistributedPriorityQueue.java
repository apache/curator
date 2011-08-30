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

import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.recipes.BaseClassForTests;
import com.netflix.curator.retry.RetryOneTime;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
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
            queue = QueueBuilder.builder(client, new IntSerializer(), "/test").maxInternalQueue(0).buildPriorityQueue(3);
            queue.start();

            for ( int i = 0; i < 10; ++i )
            {
                queue.put(i, 10 + i);
            }

            Assert.assertEquals(queue.take(1, TimeUnit.SECONDS), new Integer(0));
            queue.put(1000, 1); // lower priority
            Assert.assertEquals(queue.take(1, TimeUnit.SECONDS), new Integer(1));      // was sitting in put()
            Assert.assertEquals(queue.take(1, TimeUnit.SECONDS), new Integer(2));      // because of minItemsBeforeRefresh
            Assert.assertEquals(queue.take(1, TimeUnit.SECONDS), new Integer(1000));   // minItemsBeforeRefresh has expired
        }
        finally
        {
            Closeables.closeQuietly(queue);
            Closeables.closeQuietly(client);
        }
    }

    @Test
    public void     testSortingWhileTaking() throws Exception
    {
        DistributedPriorityQueue<Integer>   queue = null;
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            queue = QueueBuilder.builder(client, new IntSerializer(), "/test").maxInternalQueue(0).buildPriorityQueue(0);
            queue.start();

            for ( int i = 0; i < 10; ++i )
            {
                queue.put(i, 10);
            }

            Assert.assertEquals(queue.take(1, TimeUnit.SECONDS), new Integer(0));
            queue.put(1000, 1); // lower priority
            Assert.assertEquals(queue.take(1, TimeUnit.SECONDS), new Integer(1));   // was sitting in put()
            Assert.assertEquals(queue.take(1, TimeUnit.SECONDS), new Integer(1000));
        }
        finally
        {
            Closeables.closeQuietly(queue);
            Closeables.closeQuietly(client);
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
            queue = QueueBuilder.builder(client, serializer, "/test").maxInternalQueue(1).buildPriorityQueue(1);
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

            assertOrdering(queue, 10);
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
        List<Integer>                       nums = new ArrayList<Integer>();

        DistributedPriorityQueue<Integer>   queue = null;
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            queue = QueueBuilder.builder(client, new IntSerializer(), "/test").maxInternalQueue(0).buildPriorityQueue(0);
            queue.start();

            nums.add(Integer.MIN_VALUE);
            queue.put(Integer.MIN_VALUE, Integer.MIN_VALUE);  // the queue background thread will be blocking with the first item - make sure it's the lowest value

            Random          random = new Random();
            for ( int i = 0; i < 100; ++i )
            {
                int     priority = random.nextInt();
                nums.add(priority);
                queue.put(priority, priority);
            }

            assertOrdering(queue, nums.size());
        }
        catch ( AssertionError e )
        {
            StringBuilder   message = new StringBuilder(e.getMessage());
            for ( int i : nums )
            {
                message.append(i).append("\t").append(DistributedPriorityQueue.defaultPriorityToString(i)).append("\n");
            }
            Assert.fail(message.toString());
        }
        finally
        {
            Closeables.closeQuietly(queue);
            Closeables.closeQuietly(client);
        }
    }

    private void assertOrdering(DistributedPriorityQueue<Integer> queue, int qty) throws Exception
    {
        int         previous = 0;
        for ( int i = 0; i < qty; ++i )
        {
            Integer     value = queue.take(10, TimeUnit.SECONDS);
            Assert.assertNotNull(value);
            if ( i > 0 )
            {
                Assert.assertTrue(value >= previous, String.format("Value: (%d:%s) Previous: (%d:%s)", value, DistributedPriorityQueue.defaultPriorityToString(value), previous, DistributedPriorityQueue.defaultPriorityToString(previous)));
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
