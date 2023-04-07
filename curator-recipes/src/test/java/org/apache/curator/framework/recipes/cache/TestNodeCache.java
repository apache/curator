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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.curator.framework.imps.TestCleanState;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.compatibility.CuratorTestBase;
import org.apache.curator.test.compatibility.Timing2;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.Timing;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Exchanger;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@Tag(CuratorTestBase.zk35TestCompatibilityGroup)
public class TestNodeCache extends BaseClassForTests
{
    @Test
    public void     testDeleteThenCreate() throws Exception
    {
        NodeCache           cache = null;
        CuratorFramework    client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            client.create().creatingParentsIfNeeded().forPath("/test/foo", "one".getBytes());

            final AtomicReference<Throwable>        error = new AtomicReference<Throwable>();
            client.getUnhandledErrorListenable().addListener
            (
                new UnhandledErrorListener()
                {
                    @Override
                    public void unhandledError(String message, Throwable e)
                    {
                        error.set(e);
                    }
                }
            );

            final Semaphore         semaphore = new Semaphore(0);
            cache = new NodeCache(client, "/test/foo");
            cache.getListenable().addListener
            (
                new NodeCacheListener()
                {
                    @Override
                    public void nodeChanged() throws Exception
                    {
                        semaphore.release();
                    }
                }
            );
            cache.start(true);

            assertArrayEquals(cache.getCurrentData().getData(), "one".getBytes());

            client.delete().forPath("/test/foo");
            assertTrue(semaphore.tryAcquire(1, 10, TimeUnit.SECONDS));
            client.create().forPath("/test/foo", "two".getBytes());
            assertTrue(semaphore.tryAcquire(1, 10, TimeUnit.SECONDS));

            Throwable t = error.get();
            if ( t != null )
            {
                fail("Assert", t);
            }

            assertArrayEquals(cache.getCurrentData().getData(), "two".getBytes());

            cache.close();
        }
        finally
        {
            CloseableUtils.closeQuietly(cache);
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
    public void     testRebuildAgainstOtherProcesses() throws Exception
    {
        Timing2                 timing2 = new Timing2();
        NodeCache               cache = null;
        final CuratorFramework  client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            client.create().forPath("/test");
            client.create().forPath("/test/snafu", "original".getBytes());

            final CountDownLatch    latch = new CountDownLatch(1);
            cache = new NodeCache(client, "/test/snafu");
            cache.getListenable().addListener
            (
                new NodeCacheListener()
                {
                    @Override
                    public void nodeChanged() throws Exception
                    {
                        latch.countDown();
                    }
                }
            );
            cache.rebuildTestExchanger = new Exchanger<Object>();

            ExecutorService                 service = Executors.newSingleThreadExecutor();
            final NodeCache                 finalCache = cache;
            Future<Object>                  future = service.submit
            (
                new Callable<Object>()
                {
                    @Override
                    public Object call() throws Exception
                    {
                        finalCache.rebuildTestExchanger.exchange(new Object(), timing2.forWaiting().seconds(), TimeUnit.SECONDS);

                        // simulate another process updating the node while we're rebuilding
                        client.setData().forPath("/test/snafu", "other".getBytes());

                        ChildData       currentData = finalCache.getCurrentData();
                        assertNotNull(currentData);

                        finalCache.rebuildTestExchanger.exchange(new Object(), timing2.forWaiting().seconds(), TimeUnit.SECONDS);

                        return null;
                    }
                }
            );
            cache.start(false);
            future.get();

            assertTrue(timing2.awaitLatch(latch));
            assertNotNull(cache.getCurrentData());
            assertArrayEquals(cache.getCurrentData().getData(), "other".getBytes());
        }
        finally
        {
            CloseableUtils.closeQuietly(cache);
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
    public void     testKilledSession() throws Exception
    {
        NodeCache           cache = null;
        Timing              timing = new Timing();
        CuratorFramework    client = null;
        try
        {
            client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
            client.start();
            client.create().creatingParentsIfNeeded().forPath("/test/node", "start".getBytes());

            cache = new NodeCache(client, "/test/node");
            cache.start(true);

            final CountDownLatch         latch = new CountDownLatch(1);
            cache.getListenable().addListener
            (
                new NodeCacheListener()
                {
                    @Override
                    public void nodeChanged() throws Exception
                    {
                        latch.countDown();
                    }
                }
            );

            client.getZookeeperClient().getZooKeeper().getTestable().injectSessionExpiration();
            Thread.sleep(timing.multiple(1.5).session());

            assertArrayEquals(cache.getCurrentData().getData(), "start".getBytes());

            client.setData().forPath("/test/node", "new data".getBytes());
            assertTrue(timing.awaitLatch(latch));
        }
        finally
        {
            CloseableUtils.closeQuietly(cache);
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
    public void     testBasics() throws Exception
    {
        NodeCache           cache = null;
        Timing              timing = new Timing();
        CuratorFramework    client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        client.start();
        try
        {
            client.create().forPath("/test");

            cache = new NodeCache(client, "/test/node");
            cache.start(true);

            final Semaphore     semaphore = new Semaphore(0);
            cache.getListenable().addListener
            (
                new NodeCacheListener()
                {
                    @Override
                    public void nodeChanged() throws Exception
                    {
                        semaphore.release();
                    }
                }
            );

            assertNull(cache.getCurrentData());

            client.create().forPath("/test/node", "a".getBytes());
            assertTrue(timing.acquireSemaphore(semaphore));
            assertArrayEquals(cache.getCurrentData().getData(), "a".getBytes());

            client.setData().forPath("/test/node", "b".getBytes());
            assertTrue(timing.acquireSemaphore(semaphore));
            assertArrayEquals(cache.getCurrentData().getData(), "b".getBytes());

            client.delete().forPath("/test/node");
            assertTrue(timing.acquireSemaphore(semaphore));
            assertNull(cache.getCurrentData());
        }
        finally
        {
            CloseableUtils.closeQuietly(cache);
            TestCleanState.closeAndTestClean(client);
        }
    }
}
