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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.google.common.collect.Sets;
import org.apache.commons.math.stat.descriptive.SummaryStatistics;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.junit.jupiter.api.Test;

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
                assertTrue(numChildren > 0);
                assertTrue(numChildren >= (threshold * .1));
                statistics.addValue(numChildren);
            }
            latch.countDown();

            assertTrue(statistics.getMean() >= (threshold * .9));
        }
        finally
        {
            timing.sleepABit(); // let queue clear
            CloseableUtils.closeQuietly(sharder);
            CloseableUtils.closeQuietly(client);
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
            timing.sleepABit();

            assertTrue((sharder1.getShardQty() > 1) || (sharder2.getShardQty() > 1));
            timing.forWaiting().sleepABit();
            assertEquals(sharder1.getShardQty(), sharder2.getShardQty());
        }
        finally
        {
            timing.sleepABit(); // let queues clear
            CloseableUtils.closeQuietly(sharder1);
            CloseableUtils.closeQuietly(sharder2);
            CloseableUtils.closeQuietly(client);
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

            assertTrue(sharder.getShardQty() > 1);

            Set<String>             consumed = Sets.newHashSet();
            for ( int i = 0; i < 8; ++i )
            {
                String s = consumer.take(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS);
                assertNotNull(s);
                consumed.add(s);
            }

            assertEquals(consumed, Sets.newHashSet("one", "two", "three", "four", "five", "six", "seven", "eight"));

            int         shardQty = sharder.getShardQty();
            sharder.close();

            // check re-open

            sharder = new QueueSharder<String, DistributedQueue<String>>(client, distributedQueueAllocator, "/queues", "/leader", policies);
            sharder.start();
            assertEquals(sharder.getShardQty(), shardQty);
        }
        finally
        {
            CloseableUtils.closeQuietly(sharder);
            CloseableUtils.closeQuietly(client);
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
