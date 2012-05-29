package com.netflix.curator.framework.recipes.queue;

import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.recipes.BaseClassForTests;
import com.netflix.curator.framework.state.ConnectionState;
import com.netflix.curator.framework.state.ConnectionStateListener;
import com.netflix.curator.retry.RetryOneTime;
import com.netflix.curator.test.Timing;
import junit.framework.Assert;
import org.testng.annotations.Test;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class TestQueueSharder extends BaseClassForTests
{
    @Test
    public void     testSimpleDistributedQueue() throws Exception
    {
        Timing                  timing = new Timing();
        CuratorFramework        client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));

        ConnectionStateListener         connectionStateListener = new ConnectionStateListener()
        {
            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState)
            {
            }
        };

        final CountDownLatch                    latch = new CountDownLatch(1);
        final BlockingQueueConsumer<String>     consumer = new BlockingQueueConsumer<String>(connectionStateListener)
        {
            @Override
            public void consumeMessage(String message) throws Exception
            {
                latch.await();
                super.consumeMessage(message);
            }
        };
        final QueueSerializer<String>   serializer = new QueueSerializer<String>()
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
        QueueAllocator<String, DistributedQueue<String>> distributedQueueAllocator = new QueueAllocator<String, DistributedQueue<String>>()
        {
            @Override
            public DistributedQueue<String> allocateQueue(CuratorFramework client, String queuePath)
            {
                return QueueBuilder.<String>builder(client, consumer, serializer, queuePath).buildQueue();
            }
        };
        QueueSharder<String, DistributedQueue<String>>  sharder = new QueueSharder<String, DistributedQueue<String>>(client, distributedQueueAllocator, "/queues", "/leader", 2, 1);
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

            sharder = new QueueSharder<String, DistributedQueue<String>>(client, distributedQueueAllocator, "/queues", "/leader", 2, 1);
            sharder.start();
            Assert.assertEquals(sharder.getShardQty(), shardQty);
        }
        finally
        {
            Closeables.closeQuietly(sharder);
            Closeables.closeQuietly(client);
        }
    }
}
