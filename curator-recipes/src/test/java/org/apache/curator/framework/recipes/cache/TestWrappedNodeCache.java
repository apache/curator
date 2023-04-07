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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.TestCleanState;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.compatibility.CuratorTestBase;
import org.apache.curator.utils.CloseableUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.function.Supplier;

import static org.apache.curator.framework.recipes.cache.CuratorCacheListener.builder;

@Tag(CuratorTestBase.zk36Group)
public class TestWrappedNodeCache extends CuratorTestBase
{
    @Test
    public void testDeleteThenCreate() throws Exception
    {
        CuratorCache cache = null;
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();
            client.create().creatingParentsIfNeeded().forPath("/test/foo", "one".getBytes());

            final Semaphore semaphore = new Semaphore(0);
            cache = CuratorCache.build(client, "/test/foo");
            NodeCacheListener listener = semaphore::release;
            cache.listenable().addListener(builder().forNodeCache(listener).build());

            Supplier<Optional<ChildData>> rootData = getRootDataProc(cache, "/test/foo");

            cache.start();
            assertTrue(timing.acquireSemaphore(semaphore));

            assertTrue(rootData.get().isPresent());
            assertArrayEquals(rootData.get().get().getData(), "one".getBytes());

            client.delete().forPath("/test/foo");
            assertTrue(timing.acquireSemaphore(semaphore));
            client.create().forPath("/test/foo", "two".getBytes());
            assertTrue(timing.acquireSemaphore(semaphore));

            assertTrue(rootData.get().isPresent());
            assertArrayEquals(rootData.get().get().getData(), "two".getBytes());
        }
        finally
        {
            CloseableUtils.closeQuietly(cache);
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
    public void testKilledSession() throws Exception
    {
        CuratorCache cache = null;
        CuratorFramework client = null;
        try
        {
            client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
            client.start();
            client.create().creatingParentsIfNeeded().forPath("/test/node", "start".getBytes());

            CountDownLatch lostLatch = new CountDownLatch(1);
            client.getConnectionStateListenable().addListener((__, newState) -> {
                if ( newState == ConnectionState.LOST )
                {
                    lostLatch.countDown();
                }
            });

            cache = CuratorCache.build(client,"/test/node");

            Semaphore latch = new Semaphore(0);
            NodeCacheListener listener = latch::release;
            cache.listenable().addListener(builder().forNodeCache(listener).build());

            Supplier<Optional<ChildData>> rootData = getRootDataProc(cache, "/test/node");

            cache.start();
            assertTrue(timing.acquireSemaphore(latch));

            client.getZookeeperClient().getZooKeeper().getTestable().injectSessionExpiration();
            assertTrue(timing.awaitLatch(lostLatch));

            assertTrue(rootData.get().isPresent());
            assertArrayEquals(rootData.get().get().getData(), "start".getBytes());

            client.setData().forPath("/test/node", "new data".getBytes());
            assertTrue(timing.acquireSemaphore(latch));
            assertTrue(rootData.get().isPresent());
            assertArrayEquals(rootData.get().get().getData(), "new data".getBytes());
        }
        finally
        {
            CloseableUtils.closeQuietly(cache);
            TestCleanState.closeAndTestClean(client);
        }
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testBasics() throws Exception
    {
        CuratorCache cache = null;
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        try
        {
            client.start();
            client.create().forPath("/test");

            cache = CuratorCache.build(client, "/test/node");
            cache.start();

            Supplier<Optional<ChildData>> rootData = getRootDataProc(cache, "/test/node");

            final Semaphore semaphore = new Semaphore(0);
            NodeCacheListener listener = semaphore::release;
            cache.listenable().addListener(builder().forNodeCache(listener).build());

            assertNull(rootData.get().orElse(null));

            client.create().forPath("/test/node", "a".getBytes());
            assertTrue(timing.acquireSemaphore(semaphore));
            assertArrayEquals(rootData.get().orElse(null).getData(), "a".getBytes());

            client.setData().forPath("/test/node", "b".getBytes());
            assertTrue(timing.acquireSemaphore(semaphore));
            assertArrayEquals(rootData.get().orElse(null).getData(), "b".getBytes());

            client.delete().forPath("/test/node");
            assertTrue(timing.acquireSemaphore(semaphore));
            assertNull(rootData.get().orElse(null));
        }
        finally
        {
            CloseableUtils.closeQuietly(cache);
            TestCleanState.closeAndTestClean(client);
        }
    }

    private Supplier<Optional<ChildData>> getRootDataProc(CuratorCache cache, String rootPath)
    {
        return () -> cache.get(rootPath);
    }
}
