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
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.imps.TestCleanState;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.KillSession;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

public class TestNodeCache extends BaseClassForTests
{
    private final Timing timing = new Timing();

    @Test
    public void testDeleteThenCreate() throws Exception
    {
        CuratorCache cache = null;
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            client.create().creatingParentsIfNeeded().forPath("/test/foo", "one".getBytes());

            final AtomicReference<Throwable> error = new AtomicReference<Throwable>();
            client.getUnhandledErrorListenable().addListener(new UnhandledErrorListener()
            {
                @Override
                public void unhandledError(String message, Throwable e)
                {
                    error.set(e);
                }
            });

            final BlockingQueue<CacheEvent> events = new LinkedBlockingQueue<>();
            cache = CuratorCacheBuilder.builder(client, "/test/foo").forSingleNode().sendingRefreshEvents(false).build();
            cache.getListenable().addListener(new CacheListener()
            {
                @Override
                public void process(CacheEvent event, String path, CachedNode affectedNode)
                {
                    events.add(event);
                }
            });
            Assert.assertTrue(timing.awaitLatch(cache.start()));

            Assert.assertEquals(cache.getMain().getData(), "one".getBytes());

            Assert.assertEquals(events.poll(5, TimeUnit.SECONDS), CacheEvent.NODE_CREATED);
            client.delete().forPath("/test/foo");
            Assert.assertEquals(events.poll(5, TimeUnit.SECONDS), CacheEvent.NODE_DELETED);
            client.create().forPath("/test/foo", "two".getBytes());
            Assert.assertEquals(events.poll(5, TimeUnit.SECONDS), CacheEvent.NODE_CREATED);

            Throwable t = error.get();
            if ( t != null )
            {
                Assert.fail("Assert", t);
            }

            Assert.assertEquals(cache.getMain().getData(), "two".getBytes());
        }
        finally
        {
            CloseableUtils.closeQuietly(cache);
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testRebuildAgainstOtherProcesses() throws Exception
    {
        CuratorCache cache = null;
        final CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            client.create().forPath("/test");
            client.create().forPath("/test/snafu", "original".getBytes());

            final CountDownLatch latch = new CountDownLatch(1);
            cache = CuratorCacheBuilder.builder(client, "/test/snafu").forSingleNode().sendingRefreshEvents(false).build();
            cache.getListenable().addListener(new CacheListener()
            {
                @Override
                public void process(CacheEvent event, String path, CachedNode affectedNode)
                {
                    latch.countDown();
                }
            });
            ((InternalNodeCache)cache).rebuildTestExchanger = new Exchanger<Object>();

            ExecutorService service = Executors.newSingleThreadExecutor();
            final CuratorCache finalCache = cache;
            Future<Object> future = service.submit(new Callable<Object>()
            {
                @Override
                public Object call() throws Exception
                {
                    ((InternalNodeCache)finalCache).rebuildTestExchanger.exchange(new Object(), 10, TimeUnit.SECONDS);

                    // simulate another process updating the node while we're rebuilding
                    client.setData().forPath("/test/snafu", "other".getBytes());

                    CachedNode currentData = finalCache.getMain();
                    Assert.assertNotNull(currentData);

                    ((InternalNodeCache)finalCache).rebuildTestExchanger.exchange(new Object(), 10, TimeUnit.SECONDS);

                    return null;
                }
            });
            cache.start();
            future.get();

            Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
            Assert.assertNotNull(cache.getMain());
            Assert.assertEquals(cache.getMain().getData(), "other".getBytes());
        }
        finally
        {
            CloseableUtils.closeQuietly(cache);
            CloseableUtils.closeQuietly(client);
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

            cache = CuratorCacheBuilder.builder(client, "/test/node").forSingleNode().sendingRefreshEvents(false).build();

            final CountDownLatch latch = new CountDownLatch(1);
            cache.getListenable().addListener(new CacheListener()
            {
                @Override
                public void process(CacheEvent event, String path, CachedNode affectedNode)
                {
                    latch.countDown();
                }
            });
            Assert.assertTrue(timing.awaitLatch(cache.start()));

            KillSession.kill(client.getZookeeperClient().getZooKeeper());
            Thread.sleep(timing.multiple(1.5).session());

            Assert.assertEquals(cache.getMain().getData(), "start".getBytes());

            client.setData().forPath("/test/node", "new data".getBytes());
            Assert.assertTrue(timing.awaitLatch(latch));
        }
        finally
        {
            CloseableUtils.closeQuietly(cache);
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testBasics() throws Exception
    {
        CuratorCache cache = null;
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        client.start();
        try
        {
            client.create().forPath("/test");

            cache = CuratorCacheBuilder.builder(client, "/test/node").forSingleNode().sendingRefreshEvents(false).build();

            final Semaphore semaphore = new Semaphore(0);
            cache.getListenable().addListener(new CacheListener()
            {
                @Override
                public void process(CacheEvent event, String path, CachedNode affectedNode)
                {
                    semaphore.release();
                }
            });
            Assert.assertTrue(timing.awaitLatch(cache.start()));

            Assert.assertNull(cache.getMain());

            client.create().forPath("/test/node", "a".getBytes());
            Assert.assertTrue(timing.acquireSemaphore(semaphore));
            Assert.assertEquals(cache.getMain().getData(), "a".getBytes());

            client.setData().forPath("/test/node", "b".getBytes());
            Assert.assertTrue(timing.acquireSemaphore(semaphore));
            Assert.assertEquals(cache.getMain().getData(), "b".getBytes());

            client.delete().forPath("/test/node");
            Assert.assertTrue(timing.acquireSemaphore(semaphore));
            Assert.assertNull(cache.getMain());
        }
        finally
        {
            CloseableUtils.closeQuietly(cache);
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testRefreshEvent() throws Exception
    {
        CuratorCache cache = null;
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        client.start();
        try
        {
            client.create().forPath("/test");

            cache = CuratorCacheBuilder.builder(client, "/test/node").forSingleNode().build();

            final BlockingQueue<CacheEvent> events = new LinkedBlockingQueue<>();
            cache.getListenable().addListener(new CacheListener()
            {
                @Override
                public void process(CacheEvent event, String path, CachedNode affectedNode)
                {
                    events.add(event);
                }
            });
            cache.start();
            Assert.assertEquals(events.poll(timing.milliseconds(), TimeUnit.MILLISECONDS), CacheEvent.CACHE_REFRESHED);
        }
        finally
        {
            CloseableUtils.closeQuietly(cache);
            CloseableUtils.closeQuietly(client);
        }
    }
}
