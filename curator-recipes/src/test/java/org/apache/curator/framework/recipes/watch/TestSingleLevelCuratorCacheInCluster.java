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
package org.apache.curator.framework.recipes.watch;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingCluster;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class TestSingleLevelCuratorCacheInCluster
{
    private static final Timing timing = new Timing();

    @Test
    public void testServerLoss() throws Exception
    {
        CuratorFramework client = null;
        CuratorCache cache = null;
        TestingCluster cluster = new TestingCluster(3);
        try
        {
            cluster.start();

            client = CuratorFrameworkFactory.newClient(cluster.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
            client.start();
            client.create().creatingParentsIfNeeded().forPath("/test");

            cache = CuratorCacheBuilder.builder(client, "/test").withCacheSelector(CacheSelectors.singleLevel()).build();

            final CountDownLatch resetLatch = new CountDownLatch(1);
            final CountDownLatch reconnectLatch = new CountDownLatch(1);
            final CountDownLatch deleteLatch = new CountDownLatch(1);
            final CountDownLatch latch = new CountDownLatch(3);
            client.getConnectionStateListenable().addListener(new ConnectionStateListener()
            {
                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState)
                {
                    if ( newState == ConnectionState.SUSPENDED )
                    {
                        resetLatch.countDown();
                    }
                    if ( newState.isConnected() )
                    {
                        reconnectLatch.countDown();
                    }
                }
            });
            cache.getListenable().addListener(new CacheListener()
            {
                @Override
                public void process(CacheEvent event, String path, CachedNode affectedNode)
                {
                    if ( event == CacheEvent.NODE_CREATED )
                    {
                        latch.countDown();
                    }
                    else if ( event == CacheEvent.NODE_DELETED )
                    {
                        deleteLatch.countDown();
                    }
                }
            });
            cache.start();

            client.create().forPath("/test/one");
            client.create().forPath("/test/two");
            client.create().forPath("/test/three");

            Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));

            InstanceSpec connectionInstance = cluster.findConnectionInstance(client.getZookeeperClient().getZooKeeper());
            cluster.killServer(connectionInstance);

            Assert.assertTrue(timing.awaitLatch(resetLatch));
            Assert.assertEquals(cache.size(), 3);

            Assert.assertTrue(timing.awaitLatch(reconnectLatch));
            Assert.assertEquals(cache.size(), 3);

            Assert.assertEquals(deleteLatch.getCount(), 1);
            client.delete().forPath("/test/two");
            Assert.assertTrue(timing.awaitLatch(deleteLatch));
        }
        finally
        {
            CloseableUtils.closeQuietly(cache);
            CloseableUtils.closeQuietly(client);
            CloseableUtils.closeQuietly(cluster);
        }
    }
}
