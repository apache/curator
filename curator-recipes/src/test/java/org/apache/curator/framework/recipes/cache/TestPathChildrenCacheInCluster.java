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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.google.common.collect.Queues;
import io.github.artsok.RepeatedIfExceptionsTest;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.compatibility.CuratorTestBase;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingCluster;
import org.apache.curator.test.Timing;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@Tag(CuratorTestBase.zk35TestCompatibilityGroup)
public class TestPathChildrenCacheInCluster extends BaseClassForTests
{
    @RepeatedIfExceptionsTest(repeats = BaseClassForTests.REPEATS)
    @Disabled  // this test is very flakey - it needs to be re-written at some point
    public void testMissedDelete() throws Exception
    {
        Timing timing = new Timing();
        PathChildrenCache cache = null;
        CuratorFramework client1 = null;
        CuratorFramework client2 = null;
        TestingCluster cluster = createAndStartCluster(3);
        try
        {
            // client 1 only connects to 1 server
            InstanceSpec client1Instance = cluster.getInstances().iterator().next();
            client1 = CuratorFrameworkFactory.newClient(client1Instance.getConnectString(), 1000, 1000, new RetryOneTime(1));
            cache = new PathChildrenCache(client1, "/test", true);
            final BlockingQueue<PathChildrenCacheEvent.Type> events = Queues.newLinkedBlockingQueue();
            PathChildrenCacheListener listener = new PathChildrenCacheListener()
            {
                @Override
                public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
                {
                    events.add(event.getType());
                }
            };
            cache.getListenable().addListener(listener);

            client2 = CuratorFrameworkFactory.newClient(cluster.getConnectString(), 1000, 1000, new RetryOneTime(1));

            client1.start();
            client2.start();
            cache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
            assertEquals(events.poll(timing.milliseconds(), TimeUnit.MILLISECONDS), PathChildrenCacheEvent.Type.CONNECTION_RECONNECTED);
            assertEquals(events.poll(timing.milliseconds(), TimeUnit.MILLISECONDS), PathChildrenCacheEvent.Type.INITIALIZED);

            client2.create().creatingParentsIfNeeded().forPath("/test/node", "first".getBytes());
            assertEquals(events.poll(timing.milliseconds(), TimeUnit.MILLISECONDS), PathChildrenCacheEvent.Type.CHILD_ADDED);

            cluster.killServer(client1Instance);
            assertEquals(events.poll(timing.milliseconds(), TimeUnit.MILLISECONDS), PathChildrenCacheEvent.Type.CONNECTION_SUSPENDED);
            assertEquals(events.poll(timing.milliseconds(), TimeUnit.MILLISECONDS), PathChildrenCacheEvent.Type.CONNECTION_LOST);

            client2.delete().forPath("/test/node");
            client2.create().forPath("/test/node", "second".getBytes());
            cluster.restartServer(client1Instance);

            assertEquals(events.poll(timing.milliseconds(), TimeUnit.MILLISECONDS), PathChildrenCacheEvent.Type.CONNECTION_RECONNECTED);
            assertEquals(events.poll(timing.milliseconds(), TimeUnit.MILLISECONDS), PathChildrenCacheEvent.Type.CHILD_UPDATED);  // "/test/node" is different - should register as updated
        }
        finally
        {
            CloseableUtils.closeQuietly(client1);
            CloseableUtils.closeQuietly(client2);
            CloseableUtils.closeQuietly(cache);
            CloseableUtils.closeQuietly(cluster);
        }
    }

    @RepeatedIfExceptionsTest(repeats = BaseClassForTests.REPEATS)
    public void     testServerLoss() throws Exception
    {
        Timing                  timing = new Timing();

        CuratorFramework client = null;
        PathChildrenCache cache = null;
        TestingCluster cluster = createAndStartCluster(3);
        try
        {
            client = CuratorFrameworkFactory.newClient(cluster.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
            client.start();
            client.create().creatingParentsIfNeeded().forPath("/test");

            cache = new PathChildrenCache(client, "/test", false);
            cache.start();

            final CountDownLatch                    resetLatch = new CountDownLatch(1);
            final CountDownLatch                    reconnectLatch = new CountDownLatch(1);
            final AtomicReference<CountDownLatch>   latch = new AtomicReference<CountDownLatch>(new CountDownLatch(3));
            cache.getListenable().addListener
                (
                    new PathChildrenCacheListener()
                    {
                        @Override
                        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
                        {
                            if ( event.getType() == PathChildrenCacheEvent.Type.CONNECTION_SUSPENDED )
                            {
                                resetLatch.countDown();
                            }
                            else if ( event.getType() == PathChildrenCacheEvent.Type.CONNECTION_RECONNECTED )
                            {
                                reconnectLatch.countDown();
                            }
                            else if ( event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED )
                            {
                                latch.get().countDown();
                            }
                        }
                    }
                );

            client.create().forPath("/test/one");
            client.create().forPath("/test/two");
            client.create().forPath("/test/three");

            assertTrue(latch.get().await(10, TimeUnit.SECONDS));

            InstanceSpec connectionInstance = cluster.findConnectionInstance(client.getZookeeperClient().getZooKeeper());
            cluster.killServer(connectionInstance);

            assertTrue(timing.awaitLatch(reconnectLatch));

            assertEquals(cache.getCurrentData().size(), 3);
        }
        finally
        {
            CloseableUtils.closeQuietly(cache);
            CloseableUtils.closeQuietly(client);
            CloseableUtils.closeQuietly(cluster);
        }
    }
}
