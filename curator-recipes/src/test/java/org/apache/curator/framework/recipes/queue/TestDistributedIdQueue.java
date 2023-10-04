/*
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
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.utils.CloseableUtils;
import org.junit.jupiter.api.Test;

public class TestDistributedIdQueue extends BaseClassForTests {
    private static final String QUEUE_PATH = "/a/queue";

    private static final QueueSerializer<TestQueueItem> serializer = new QueueItemSerializer();

    @Test
    public void testDeletingWithLock() throws Exception {
        DistributedIdQueue<TestQueueItem> queue = null;
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try {
            final CountDownLatch consumingLatch = new CountDownLatch(1);
            final CountDownLatch waitLatch = new CountDownLatch(1);
            QueueConsumer<TestQueueItem> consumer = new QueueConsumer<TestQueueItem>() {
                @Override
                public void consumeMessage(TestQueueItem message) throws Exception {
                    consumingLatch.countDown();
                    waitLatch.await();
                }

                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState) {}
            };

            queue = QueueBuilder.builder(client, consumer, serializer, QUEUE_PATH)
                    .lockPath("/locks")
                    .buildIdQueue();
            queue.start();

            queue.put(new TestQueueItem("test"), "id");

            assertTrue(consumingLatch.await(10, TimeUnit.SECONDS)); // wait until consumer has it
            assertEquals(queue.remove("id"), 0);

            waitLatch.countDown();
        } finally {
            CloseableUtils.closeQuietly(queue);
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testOrdering() throws Exception {
        final int ITEM_QTY = 100;

        DistributedIdQueue<TestQueueItem> queue = null;
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try {
            BlockingQueueConsumer<TestQueueItem> consumer =
                    new BlockingQueueConsumer<>((curatorFramework, connectionState) -> {});

            queue = QueueBuilder.builder(client, consumer, serializer, QUEUE_PATH)
                    .buildIdQueue();
            queue.start();

            List<String> ids = Lists.newArrayList();
            for (int i = 0; i < ITEM_QTY; ++i) {
                String id = Double.toString(Math.random());
                ids.add(id);
                queue.put(new TestQueueItem(id), id);
            }

            int iteration = 0;
            while (consumer.size() < ITEM_QTY) {
                assertTrue(++iteration < ITEM_QTY);
                Thread.sleep(1000);
            }

            int i = 0;
            for (TestQueueItem item : consumer.getItems()) {
                assertEquals(item.str, ids.get(i++));
            }
        } finally {
            CloseableUtils.closeQuietly(queue);
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testRequeuingWithLock() throws Exception {
        DistributedIdQueue<TestQueueItem> queue = null;
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try {
            final CountDownLatch consumingLatch = new CountDownLatch(1);

            QueueConsumer<TestQueueItem> consumer = new QueueConsumer<TestQueueItem>() {
                @Override
                public void consumeMessage(TestQueueItem message) throws Exception {
                    consumingLatch.countDown();
                    // Throw an exception so requeuing occurs
                    throw new Exception("Consumer failed");
                }

                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState) {}
            };

            queue = QueueBuilder.builder(client, consumer, serializer, QUEUE_PATH)
                    .lockPath("/locks")
                    .buildIdQueue();
            queue.start();

            queue.put(new TestQueueItem("test"), "id");

            assertTrue(consumingLatch.await(10, TimeUnit.SECONDS)); // wait until consumer has it

            // Sleep one more second

            Thread.sleep(1000);

            assertTrue(queue.debugIsQueued("id"));

        } finally {
            CloseableUtils.closeQuietly(queue);
            CloseableUtils.closeQuietly(client);
        }
    }
}
