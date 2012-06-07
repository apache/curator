package com.netflix.curator.framework.recipes.queue;

import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.recipes.BaseClassForTests;
import com.netflix.curator.framework.state.ConnectionState;
import com.netflix.curator.retry.RetryOneTime;
import com.netflix.curator.test.Timing;
import junit.framework.Assert;
import org.testng.annotations.Test;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class TestBoundedDistributedQueue extends BaseClassForTests
{
    @Test
    public void         testSimple() throws Exception
    {
        Timing                      timing = new Timing();
        DistributedQueue<String>    queue = null;
        CuratorFramework            client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        try
        {
            client.start();

            final List<String>          messages = new CopyOnWriteArrayList<String>();
            final CountDownLatch        latch = new CountDownLatch(1);
            final Semaphore             semaphore = new Semaphore(0);
            QueueConsumer<String>       consumer = new QueueConsumer<String>()
            {
                @Override
                public void consumeMessage(String message) throws Exception
                {
                    semaphore.acquire();
                    messages.add(message);
                }

                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState)
                {
                }
            };
            QueueSerializer<String>     serializer = new QueueSerializer<String>()
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
            queue = QueueBuilder.builder(client, consumer, serializer, "/queue").executor(Executors.newSingleThreadExecutor()).maxItems(1).buildQueue();
            queue.start();

            QueuePutListener<String>    listener = new QueuePutListener<String>()
            {
                @Override
                public void putCompleted(String item)
                {
                    latch.countDown();
                }

                @Override
                public void putMultiCompleted(MultiItem<String> items)
                {
                }
            };
            queue.getPutListenerContainer().addListener(listener);

            Assert.assertTrue(queue.put("1", timing.milliseconds(), TimeUnit.MILLISECONDS));   // should end up in consumer
            Assert.assertTrue(queue.put("2", timing.milliseconds(), TimeUnit.MILLISECONDS));   // should sit blocking in DistributedQueue
            Assert.assertTrue(timing.awaitLatch(latch));
            Assert.assertFalse(queue.put("3", timing.multiple(.5).milliseconds(), TimeUnit.MILLISECONDS));

            semaphore.release(100);
            Assert.assertTrue(queue.put("3", timing.milliseconds(), TimeUnit.MILLISECONDS));
            Assert.assertTrue(queue.put("4", timing.milliseconds(), TimeUnit.MILLISECONDS));
            Assert.assertTrue(queue.put("5", timing.milliseconds(), TimeUnit.MILLISECONDS));

            for ( int i = 0; i < 5; ++i )
            {
                if ( messages.size() == 3 )
                {
                    break;
                }
                timing.sleepABit();
            }

            Assert.assertEquals(messages, Arrays.asList("1", "2", "3", "4", "5"));
        }
        finally
        {
            Closeables.closeQuietly(queue);
            Closeables.closeQuietly(client);
        }
    }
}
