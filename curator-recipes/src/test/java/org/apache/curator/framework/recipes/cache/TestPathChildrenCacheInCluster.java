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

package org.apache.curator.framework.recipes.cache;

import com.google.common.io.Closeables;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingCluster;
import org.apache.curator.test.Timing;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class TestPathChildrenCacheInCluster
{
    @Test
    public void     testServerLoss() throws Exception
    {
        Timing                  timing = new Timing();

        CuratorFramework client = null;
        PathChildrenCache cache = null;
        TestingCluster cluster = new TestingCluster(3);
        try
        {
            cluster.start();

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

            Assert.assertTrue(latch.get().await(10, TimeUnit.SECONDS));

            InstanceSpec connectionInstance = cluster.findConnectionInstance(client.getZookeeperClient().getZooKeeper());
            cluster.killServer(connectionInstance);

            Assert.assertTrue(timing.awaitLatch(reconnectLatch));

            Assert.assertEquals(cache.getCurrentData().size(), 3);
        }
        finally
        {
            Closeables.closeQuietly(cache);
            Closeables.closeQuietly(client);
            Closeables.closeQuietly(cluster);
        }
    }
}
