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

package org.apache.curator.framework.recipes.cache;

import static org.apache.curator.framework.recipes.cache.CuratorCache.Options.DO_NOT_CLEAR_ON_CLOSE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingCluster;
import org.apache.curator.test.compatibility.CuratorTestBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;

@Tag(CuratorTestBase.zk36Group)
public class TestCuratorCacheEdges extends CuratorTestBase
{
    @Test
    public void testReconnectConsistency() throws Exception
    {
        final byte[] first = "one".getBytes();
        final byte[] second = "two".getBytes();

        try (CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1)))
        {
            client.start();
            client.create().forPath("/root", first);
            client.create().forPath("/root/1", first);
            client.create().forPath("/root/2", first);
            client.create().forPath("/root/1/11", first);
            client.create().forPath("/root/1/12", first);
            client.create().forPath("/root/1/13", first);
            client.create().forPath("/root/2/21", first);
            client.create().forPath("/root/2/22", first);

            CuratorCacheStorage storage = CuratorCacheStorage.standard();
            try (CuratorCache cache = CuratorCache.builder(client, "/root").withStorage(storage).withOptions(DO_NOT_CLEAR_ON_CLOSE).build())
            {
                CountDownLatch latch = new CountDownLatch(1);
                cache.listenable().addListener(CuratorCacheListener.builder().forInitialized(latch::countDown).build());
                cache.start();
                assertTrue(timing.awaitLatch(latch));
            }

            // we now have a storage loaded with the initial nodes created

            // simulate nodes changing during a partition

            client.delete().forPath("/root/2/21");
            client.delete().forPath("/root/2/22");
            client.delete().forPath("/root/2");

            client.setData().forPath("/root", second);
            client.create().forPath("/root/1/11/111", second);
            client.create().forPath("/root/1/11/111/1111", second);
            client.create().forPath("/root/1/11/111/1112", second);
            client.create().forPath("/root/1/13/131", second);
            client.create().forPath("/root/1/13/132", second);
            client.create().forPath("/root/1/13/132/1321", second);

            try (CuratorCache cache = CuratorCache.builder(client, "/root").withStorage(storage).withOptions(DO_NOT_CLEAR_ON_CLOSE).build())
            {
                CountDownLatch latch = new CountDownLatch(1);
                cache.listenable().addListener(CuratorCacheListener.builder().forInitialized(latch::countDown).build());
                cache.start();
                assertTrue(timing.awaitLatch(latch));
            }

            assertEquals(storage.size(), 11);
            assertArrayEquals(storage.get("/root").map(ChildData::getData).orElse(null), second);
            assertArrayEquals(storage.get("/root/1").map(ChildData::getData).orElse(null), first);
            assertArrayEquals(storage.get("/root/1/11").map(ChildData::getData).orElse(null), first);
            assertArrayEquals(storage.get("/root/1/11/111").map(ChildData::getData).orElse(null), second);
            assertArrayEquals(storage.get("/root/1/11/111/1111").map(ChildData::getData).orElse(null), second);
            assertArrayEquals(storage.get("/root/1/11/111/1112").map(ChildData::getData).orElse(null), second);
            assertArrayEquals(storage.get("/root/1/12").map(ChildData::getData).orElse(null), first);
            assertArrayEquals(storage.get("/root/1/13").map(ChildData::getData).orElse(null), first);
            assertArrayEquals(storage.get("/root/1/13/131").map(ChildData::getData).orElse(null), second);
            assertArrayEquals(storage.get("/root/1/13/132").map(ChildData::getData).orElse(null), second);
            assertArrayEquals(storage.get("/root/1/13/132/1321").map(ChildData::getData).orElse(null), second);
        }
    }

    @Test
    public void testServerLoss() throws Exception   // mostly copied from TestPathChildrenCacheInCluster
    {
        try (TestingCluster cluster = new TestingCluster(3))
        {
            cluster.start();

            try (CuratorFramework client = CuratorFrameworkFactory.newClient(cluster.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1)))
            {
                client.start();
                client.create().creatingParentsIfNeeded().forPath("/test");

                try (CuratorCache cache = CuratorCache.build(client, "/test"))
                {
                    cache.start();

                    CountDownLatch reconnectLatch = new CountDownLatch(1);
                    client.getConnectionStateListenable().addListener((__, newState) -> {
                        if ( newState == ConnectionState.RECONNECTED )
                        {
                            reconnectLatch.countDown();
                        }
                    });
                    CountDownLatch latch = new CountDownLatch(3);
                    cache.listenable().addListener((__, ___, ____) -> latch.countDown());

                    client.create().forPath("/test/one");
                    client.create().forPath("/test/two");
                    client.create().forPath("/test/three");

                    assertTrue(timing.awaitLatch(latch));

                    InstanceSpec connectionInstance = cluster.findConnectionInstance(client.getZookeeperClient().getZooKeeper());
                    cluster.killServer(connectionInstance);

                    assertTrue(timing.awaitLatch(reconnectLatch));

                    timing.sleepABit();

                    assertEquals(cache.stream().count(), 4);
                }
            }
        }
    }
}
