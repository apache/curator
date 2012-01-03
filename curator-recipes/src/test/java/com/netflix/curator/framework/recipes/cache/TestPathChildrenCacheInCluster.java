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

package com.netflix.curator.framework.recipes.cache;

import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.netflix.curator.test.TestingCluster;
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
        final int               BASE_TIMEOUT = 5000;

        CuratorFramework client = null;
        PathChildrenCache       cache = null;
        TestingCluster cluster = new TestingCluster(3);
        try
        {
            cluster.start();

            client = CuratorFrameworkFactory.newClient(cluster.getConnectString(), BASE_TIMEOUT, BASE_TIMEOUT, new ExponentialBackoffRetry(BASE_TIMEOUT / 2, 3));
            client.start();
            client.create().creatingParentsIfNeeded().forPath("/test");

            cache = new PathChildrenCache(client, "/test", PathChildrenCacheMode.CACHE_PATHS_ONLY);
            cache.start();

            final AtomicReference<CountDownLatch> latch = new AtomicReference<CountDownLatch>(new CountDownLatch(3));
            cache.getListenable().addListener
            (
                new PathChildrenCacheListener()
                {
                    @Override
                    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
                    {
                        if ( event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED )
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

            latch.set(new CountDownLatch(3));
            TestingCluster.InstanceSpec connectionInstance = cluster.findConnectionInstance(client.getZookeeperClient().getZooKeeper());
            cluster.killServer(connectionInstance);

            Assert.assertTrue(latch.get().await(BASE_TIMEOUT * 2, TimeUnit.SECONDS)); // the cache should reset itself
        }
        finally
        {
            Closeables.closeQuietly(cache);
            Closeables.closeQuietly(client);
            Closeables.closeQuietly(cluster);
        }
    }
}
