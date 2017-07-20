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
package org.apache.curator.framework.recipes.cache;

import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.Timing;
import org.apache.curator.test.Timing2;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.KeeperException;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.io.Closeable;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public abstract class TestEventOrdering<T extends Closeable> extends BaseClassForTests
{
    private final Timing2 timing = new Timing2();
    private final long start = System.currentTimeMillis();
    private static final int THREAD_QTY = 100;
    private static final int ITERATIONS = 100;
    private static final int NODE_QTY = 10;

    public enum EventType
    {
        ADDED,
        DELETED
    }

    public static class Event
    {
        public final EventType eventType;
        public final String path;
        public final long time = System.currentTimeMillis();

        public Event(EventType eventType, String path)
        {
            this.eventType = eventType;
            this.path = path;
        }
    }

    @Test
    public void testEventOrdering() throws Exception
    {
        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_QTY);
        BlockingQueue<Event> events = Queues.newLinkedBlockingQueue();
        final CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        T cache = null;
        try
        {
            client.start();
            client.create().forPath("/root");
            cache = newCache(client, "/root", events);

            final Random random = new Random();
            final Callable<Void> task = new Callable<Void>()
            {
                @Override
                public Void call() throws Exception
                {
                    for ( int i = 0; i < ITERATIONS; ++i )
                    {
                        String node = "/root/" + random.nextInt(NODE_QTY);
                        try
                        {
                            switch ( random.nextInt(3) )
                            {
                            default:
                            case 0:
                                client.create().forPath(node);
                                break;

                            case 1:
                                client.setData().forPath(node, "new".getBytes());
                                break;

                            case 2:
                                client.delete().forPath(node);
                                break;
                            }
                        }
                        catch ( KeeperException ignore )
                        {
                            // ignore
                        }
                    }
                    return null;
                }
            };

            final CountDownLatch latch = new CountDownLatch(THREAD_QTY);
            for ( int i = 0; i < THREAD_QTY; ++i )
            {
                Callable<Void> wrapped = new Callable<Void>()
                {
                    @Override
                    public Void call() throws Exception
                    {
                        try
                        {
                            return task.call();
                        }
                        finally
                        {
                            latch.countDown();
                        }
                    }
                };
                executorService.submit(wrapped);
            }
            Assert.assertTrue(timing.awaitLatch(latch));

            timing.sleepABit();

            List<Event> localEvents = Lists.newArrayList();
            int eventSuggestedQty = 0;
            while ( events.size() > 0 )
            {
                Event event = timing.takeFromQueue(events);
                localEvents.add(event);
                eventSuggestedQty += (event.eventType == EventType.ADDED) ? 1 : -1;
            }
            int actualQty = getActualQty(cache);
            Assert.assertEquals(actualQty, eventSuggestedQty, String.format("actual %s expected %s:\n %s", actualQty, eventSuggestedQty, asString(localEvents)));
        }
        finally
        {
            executorService.shutdownNow();
            //noinspection ThrowFromFinallyBlock
            executorService.awaitTermination(timing.milliseconds(), TimeUnit.MILLISECONDS);
            CloseableUtils.closeQuietly(cache);
            CloseableUtils.closeQuietly(client);
        }
    }

    protected abstract int getActualQty(T cache);

    protected abstract T newCache(CuratorFramework client, String path, BlockingQueue<Event> events) throws Exception;

    private String asString(List<Event> events)
    {
        int qty = 0;
        StringBuilder str = new StringBuilder();
        for ( Event event : events )
        {
            qty += (event.eventType == EventType.ADDED) ? 1 : -1;
            str.append(event.eventType).append(" ").append(event.path).append(" @ ").append(event.time - start).append(' ').append(qty);
            str.append("\n");
        }
        return str.toString();
    }
}
