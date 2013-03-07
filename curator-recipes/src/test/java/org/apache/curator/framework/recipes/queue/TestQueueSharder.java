/*
 * Copyright 2012 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.apache.curator.framework.recipes.queue;

import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.BaseClassForTests;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.Timing;
import junit.framework.Assert;
import org.apache.commons.math.stat.descriptive.SummaryStatistics;
import org.testng.annotations.Test;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class TestQueueSharder extends BaseClassForTests
{
    @Test
    public void     testDistribution() throws Exception
    {
        final int               threshold = 100;
        final int               factor = 10;

        Timing                  timing = new Timing();
        CuratorFramework        client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        QueueSharder<String, DistributedQueue<String>>  sharder = null;
        try
        {
            client.start();

            final CountDownLatch        latch = new CountDownLatch(1);
            QueueConsumer<String>       consumer = new QueueConsumer<String>()
            {
                @Override
                public void consumeMessage(String message) throws Exception
                {
                    latch.await();
                }

                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState)
                {
                }
            };
            QueueAllocator<String, DistributedQueue<String>>    distributedQueueAllocator = makeAllocator(consumer);
            QueueSharderPolicies                                policies = QueueSharderPolicies.builder().newQueueThreshold(threshold).thresholdCheckMs(1).build();
            sharder = new QueueSharder<String, DistributedQueue<String>>(client, distributedQueueAllocator, "/queues", "/leader", policies);
            sharder.start();

            for ( int i = 0; i < (factor * threshold); ++i )
            {
                sharder.getQueue().put(Integer.toString(i));
                Thread.sleep(5);
            }
            timing.forWaiting().sleepABit();

            SummaryStatistics       statistics = new SummaryStatistics();
            for ( String path : sharder.getQueuePaths() )
            {
                int numChildren = client.checkExists().forPath(path).getNumChildren();
                Assert.assertTrue(numChildren > 0);
                Assert.assertTrue(numChildren >= (threshold * .1));
                statistics.addValue(numChildren);
            }
            latch.countDown();

            Assert.assertTrue(statistics.getMean() >= (threshold * .9));
        }
        finally
        {
            timing.sleepABit(); // let queue clear
            Closeables.closeQuietly(sharder);
            Closeables.closeQuietly(client);
        }
    }

    @Test
    public void     testSharderWatchSync() throws Exception
    {
        Timing                  timing = new Timing();
        CuratorFramework        client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));

        final BlockingQueueConsumer<String>     consumer = makeConsumer(null);
        QueueAllocator<String, DistributedQueue<String>>    distributedQueueAllocator = makeAllocator(consumer);
        QueueSharderPolicies        policies = QueueSharderPolicies.builder().newQueueThreshold(2).thresholdCheckMs(1).build();

        QueueSharder<String, DistributedQueue<String>>  sharder1 = new QueueSharder<String, DistributedQueue<String>>(client, distributedQueueAllocator, "/queues", "/leader", policies);
        QueueSharder<String, DistributedQueue<String>>  sharder2 = new QueueSharder<String, DistributedQueue<String>>(client, distributedQueueAllocator, "/queues", "/leader", policies);
        try
        {
            client.start();
            sharder1.start();
            sharder2.start();

            for ( int i = 0; i < 20; ++i )
            {
                sharder1.getQueue().put(Integer.toString(i));
            }
            timing.forWaiting().sleepABit();

            Assert.assertTrue((sharder1.getShardQty() > 1) || (sharder2.getShardQty() > 1));
            timing.forWaiting().sleepABit();
            Assert.assertEquals(sharder1.getShardQty(), sharder2.getShardQty());
        }
        finally
        {
            timing.sleepABit(); // let queues clear
            Closeables.closeQuietly(sharder1);
            Closeables.closeQuietly(sharder2);
            Closeables.closeQuietly(client);
        }
    }

    @Test
    public void     testSimpleDistributedQueue() throws Exception
    {
        Timing                  timing = new Timing();
        CuratorFramework        client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));

        final CountDownLatch                    latch = new CountDownLatch(1);
        final BlockingQueueConsumer<String>     consumer = makeConsumer(latch);
        QueueAllocator<String, DistributedQueue<String>>    distributedQueueAllocator = makeAllocator(consumer);
        QueueSharderPolicies        policies = QueueSharderPolicies.builder().newQueueThreshold(2).thresholdCheckMs(1).build();
        QueueSharder<String, DistributedQueue<String>>  sharder = new QueueSharder<String, DistributedQueue<String>>(client, distributedQueueAllocator, "/queues", "/leader", policies);
        try
        {
            client.start();
            sharder.start();

            sharder.getQueue().put("one");
            sharder.getQueue().put("two");
            sharder.getQueue().put("three");
            sharder.getQueue().put("four");
            latch.countDown();
            timing.sleepABit();
            sharder.getQueue().put("five");
            sharder.getQueue().put("six");
            sharder.getQueue().put("seven");
            sharder.getQueue().put("eight");
            timing.sleepABit();

            Assert.assertTrue(sharder.getShardQty() > 1);

            Set<String>             consumed = Sets.newHashSet();
            for ( int i = 0; i < 8; ++i )
            {
                String s = consumer.take(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS);
                Assert.assertNotNull(s);
                consumed.add(s);
            }

            Assert.assertEquals(consumed, Sets.newHashSet("one", "two", "three", "four", "five", "six", "seven", "eight"));

            int         shardQty = sharder.getShardQty();
            sharder.close();

            // check re-open

            sharder = new QueueSharder<String, DistributedQueue<String>>(client, distributedQueueAllocator, "/queues", "/leader", policies);
            sharder.start();
            Assert.assertEquals(sharder.getShardQty(), shardQty);
        }
        finally
        {
            Closeables.closeQuietly(sharder);
            Closeables.closeQuietly(client);
        }
    }

    private QueueAllocator<String, DistributedQueue<String>> makeAllocator(final QueueConsumer<String> consumer)
    {
        final QueueSerializer<String> serializer = new QueueSerializer<String>()
        {
            @Override
            public byte[] serialize(String item)
            {
                return item.getBytes();
            }

            @Override
            public String deserialize(byte[] bytes)
            {
                return new String(bytes);
            }
        };
        return new QueueAllocator<String, DistributedQueue<String>>()
        {
            @Override
            public DistributedQueue<String> allocateQueue(CuratorFramework client, String queuePath)
            {
                return QueueBuilder.<String>builder(client, consumer, serializer, queuePath).buildQueue();
            }
        };
    }

    private BlockingQueueConsumer<String> makeConsumer(final CountDownLatch latch)
    {
        ConnectionStateListener connectionStateListener = new ConnectionStateListener()
        {
            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState)
            {
            }
        };

        return new BlockingQueueConsumer<String>(connectionStateListener)
        {
            @Override
            public void consumeMessage(String message) throws Exception
            {
                if ( latch != null )
                {
                    latch.await();
                }
                super.consumeMessage(message);
            }
        };
    }
}
