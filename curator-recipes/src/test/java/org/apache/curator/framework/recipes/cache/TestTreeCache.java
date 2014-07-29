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

import com.google.common.collect.ImmutableSortedSet;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.KillSession;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class TestTreeCache extends BaseClassForTests
{
    private final Timing timing = new Timing();
    private CuratorFramework client;
    private TreeCache cache;
    private List<Throwable> exceptions;
    private BlockingQueue<TreeCacheEvent> events;
    private TreeCacheListener eventListener;

    /**
     * A TreeCache that records exceptions.
     */
    class TreeCache extends org.apache.curator.framework.recipes.cache.TreeCache {

        TreeCache(CuratorFramework client, String path, boolean cacheData)
        {
            super(client, path, cacheData);
        }

        @Override
        protected void handleException(Throwable e)
        {
            exceptions.add(e);
        }
    }

    @Override
    @BeforeMethod
    public void setup() throws Exception
    {
        super.setup();

        exceptions = new ArrayList<Throwable>();
        events = new LinkedBlockingQueue<TreeCacheEvent>();
        eventListener = new TreeCacheListener()
        {
            @Override
            public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception
            {
                if (event.getData() != null && event.getData().getPath().startsWith("/zookeeper"))
                {
                    // Suppress any events related to /zookeeper paths
                    return;
                }
                events.add(event);
            }
        };

        client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        client.start();
        client.getUnhandledErrorListenable().addListener(new UnhandledErrorListener()
        {
            @Override
            public void unhandledError(String message, Throwable e)
            {
                exceptions.add(e);
            }
        });
        cache = new TreeCache(client, "/test", true);
        cache.getListenable().addListener(eventListener);
    }

    @Override
    @AfterMethod
    public void teardown() throws Exception
    {
        try
        {
            try
            {
                if ( exceptions.size() == 1 )
                {
                    Assert.fail("Exception was thrown", exceptions.get(0));
                }
                else if ( exceptions.size() > 1 )
                {
                    AssertionError error = new AssertionError("Multiple exceptions were thrown");
                    for ( Throwable exception : exceptions )
                    {
                        error.addSuppressed(exception);
                    }
                    throw error;
                }
            }
            finally
            {
                CloseableUtils.closeQuietly(cache);
                CloseableUtils.closeQuietly(client);
            }
        }
        finally
        {
            super.teardown();
        }
    }

    @Test
    public void testStartup() throws Exception
    {
        client.create().forPath("/test");
        client.create().forPath("/test/1", "one".getBytes());
        client.create().forPath("/test/2", "two".getBytes());
        client.create().forPath("/test/3", "three".getBytes());
        client.create().forPath("/test/2/sub", "two-sub".getBytes());

        cache.start();
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test");
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/1", "one".getBytes());
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/2", "two".getBytes());
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/3", "three".getBytes());
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/2/sub", "two-sub".getBytes());
        assertEvent(TreeCacheEvent.Type.INITIALIZED);
        assertNoMoreEvents();

        Assert.assertEquals(cache.getCurrentChildren("/test"), ImmutableSortedSet.of("1", "2", "3"));
        Assert.assertEquals(cache.getCurrentChildren("/test/1"), ImmutableSortedSet.of());
        Assert.assertEquals(cache.getCurrentChildren("/test/2"), ImmutableSortedSet.of("sub"));
        Assert.assertNull(cache.getCurrentChildren("/test/non_exist"));
    }

    @Test
    public void testStartEmpty() throws Exception
    {
        cache.start();
        assertEvent(TreeCacheEvent.Type.INITIALIZED);

        client.create().forPath("/test");
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test");
        assertNoMoreEvents();
    }

    @Test
    public void testAsyncInitialPopulation() throws Exception
    {
        client.create().forPath("/test");
        client.create().forPath("/test/one", "hey there".getBytes());
        cache.start();
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test");
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/one");
        assertEvent(TreeCacheEvent.Type.INITIALIZED);
        assertNoMoreEvents();
    }

    @Test
    public void testFromRoot() throws Exception
    {
        client.create().forPath("/test");
        client.create().forPath("/test/one", "hey there".getBytes());
        cache = new TreeCache(client, "/", true);
        cache.getListenable().addListener(eventListener);
        cache.start();
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/");
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test");
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/one");
        assertEvent(TreeCacheEvent.Type.INITIALIZED);
        assertNoMoreEvents();
    }

    @Test
    public void testWithNamespace() throws Exception
    {
        client.create().forPath("/outer");
        client.create().forPath("/outer/foo");
        client.create().forPath("/outer/test");
        client.create().forPath("/outer/test/one", "hey there".getBytes());
        cache = new TreeCache(client.usingNamespace("outer"), "/test", true);
        cache.getListenable().addListener(eventListener);
        cache.start();
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test");
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/one");
        assertEvent(TreeCacheEvent.Type.INITIALIZED);
        assertNoMoreEvents();
    }

    @Test
    public void testWithNamespaceAtRoot() throws Exception
    {
        client.create().forPath("/outer");
        client.create().forPath("/outer/foo");
        client.create().forPath("/outer/test");
        client.create().forPath("/outer/test/one", "hey there".getBytes());
        cache = new TreeCache(client.usingNamespace("outer"), "/", true);
        cache.getListenable().addListener(eventListener);
        cache.start();
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/");
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/foo");
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test");
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/one");
        assertEvent(TreeCacheEvent.Type.INITIALIZED);
        assertNoMoreEvents();
    }

    @Test
    public void testSyncInitialPopulation() throws Exception
    {
        cache.start();
        assertEvent(TreeCacheEvent.Type.INITIALIZED);
        client.create().forPath("/test");
        client.create().forPath("/test/one", "hey there".getBytes());
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test");
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/one");
        assertNoMoreEvents();
    }

    @Test
    public void testChildrenInitialized() throws Exception
    {
        client.create().forPath("/test", "".getBytes());
        client.create().forPath("/test/1", "1".getBytes());
        client.create().forPath("/test/2", "2".getBytes());
        client.create().forPath("/test/3", "3".getBytes());

        cache.start();
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test");
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/1");
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/2");
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/3");
        assertEvent(TreeCacheEvent.Type.INITIALIZED);
        assertNoMoreEvents();
    }

    @Test
    public void testUpdateWhenNotCachingData() throws Exception
    {
        cache = new TreeCache(client, "/test", false);
        cache.getListenable().addListener(eventListener);

        client.create().forPath("/test");
        cache.start();
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test");
        assertEvent(TreeCacheEvent.Type.INITIALIZED);

        client.create().forPath("/test/foo", "first".getBytes());
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/foo");

        client.setData().forPath("/test/foo", "something new".getBytes());
        assertEvent(TreeCacheEvent.Type.NODE_UPDATED, "/test/foo");
        assertNoMoreEvents();
    }

    @Test
    public void testDeleteThenCreate() throws Exception
    {
        client.create().forPath("/test");
        client.create().forPath("/test/foo", "one".getBytes());
        cache.start();

        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test");
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/foo");
        assertEvent(TreeCacheEvent.Type.INITIALIZED);

        client.delete().forPath("/test/foo");
        assertEvent(TreeCacheEvent.Type.NODE_REMOVED, "/test/foo");
        client.create().forPath("/test/foo", "two".getBytes());
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/foo");

        assertNoMoreEvents();
    }

    // see https://github.com/Netflix/curator/issues/27 - was caused by not comparing old->new data
    @Test
    public void testIssue27() throws Exception
    {
        client.create().forPath("/test");
        client.create().forPath("/test/a");
        client.create().forPath("/test/b");
        client.create().forPath("/test/c");

        client.getChildren().forPath("/test");

        cache.start();
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test");
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/a");
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/b");
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/c");
        assertEvent(TreeCacheEvent.Type.INITIALIZED);

        client.delete().forPath("/test/a");
        client.create().forPath("/test/a");
        assertEvent(TreeCacheEvent.Type.NODE_REMOVED, "/test/a");
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/a");

        assertNoMoreEvents();
    }

    @Test
    public void testKilledSession() throws Exception
    {
        client.create().forPath("/test");
        cache.start();

        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test");
        assertEvent(TreeCacheEvent.Type.INITIALIZED);

        client.create().forPath("/test/foo", "foo".getBytes());
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/foo");
        client.create().withMode(CreateMode.EPHEMERAL).forPath("/test/me", "data".getBytes());
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/me");

        KillSession.kill(client.getZookeeperClient().getZooKeeper(), server.getConnectString());
        assertEvent(TreeCacheEvent.Type.CONNECTION_SUSPENDED);
        assertEvent(TreeCacheEvent.Type.CONNECTION_LOST);
        assertEvent(TreeCacheEvent.Type.CONNECTION_RECONNECTED);
        assertEvent(TreeCacheEvent.Type.NODE_REMOVED, "/test/me");

        assertNoMoreEvents();
    }

    @Test
    public void testBasics() throws Exception
    {
        client.create().forPath("/test");
        cache.start();
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test");
        assertEvent(TreeCacheEvent.Type.INITIALIZED);
        Assert.assertEquals(cache.getCurrentChildren("/test"), ImmutableSortedSet.of());

        client.create().forPath("/test/one", "hey there".getBytes());
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/one");
        Assert.assertEquals(cache.getCurrentChildren("/test"), ImmutableSortedSet.of("one"));
        Assert.assertEquals(new String(cache.getCurrentData("/test/one").getData()), "hey there");

        client.setData().forPath("/test/one", "sup!".getBytes());
        assertEvent(TreeCacheEvent.Type.NODE_UPDATED, "/test/one");
        Assert.assertEquals(cache.getCurrentChildren("/test"), ImmutableSortedSet.of("one"));
        Assert.assertEquals(new String(cache.getCurrentData("/test/one").getData()), "sup!");

        client.delete().forPath("/test/one");
        assertEvent(TreeCacheEvent.Type.NODE_REMOVED, "/test/one");
        Assert.assertEquals(cache.getCurrentChildren("/test"), ImmutableSortedSet.of());

        assertNoMoreEvents();
    }

    @Test
    public void testBasicsOnTwoCaches() throws Exception
    {
        TreeCache cache2 = new TreeCache(client, "/test", true);

        // Just ensures the same event count; enables test flow control on cache2.
        final Semaphore semaphore = new Semaphore(0);
        cache2.getListenable().addListener(new TreeCacheListener()
        {
            @Override
            public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception
            {
                semaphore.release();
            }
        });

        try
        {
            client.create().forPath("/test");
            cache.start();
            cache2.start();

            assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test");
            assertEvent(TreeCacheEvent.Type.INITIALIZED);
            semaphore.acquire(2);

            client.create().forPath("/test/one", "hey there".getBytes());
            assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/one");
            Assert.assertEquals(new String(cache.getCurrentData("/test/one").getData()), "hey there");
            semaphore.acquire();
            Assert.assertEquals(new String(cache2.getCurrentData("/test/one").getData()), "hey there");

            client.setData().forPath("/test/one", "sup!".getBytes());
            assertEvent(TreeCacheEvent.Type.NODE_UPDATED, "/test/one");
            Assert.assertEquals(new String(cache.getCurrentData("/test/one").getData()), "sup!");
            semaphore.acquire();
            Assert.assertEquals(new String(cache2.getCurrentData("/test/one").getData()), "sup!");

            client.delete().forPath("/test/one");
            assertEvent(TreeCacheEvent.Type.NODE_REMOVED, "/test/one");
            Assert.assertNull(cache.getCurrentData("/test/one"));
            semaphore.acquire();
            Assert.assertNull(cache2.getCurrentData("/test/one"));

            assertNoMoreEvents();
            Assert.assertEquals(semaphore.availablePermits(), 0);
        }
        finally
        {
            CloseableUtils.closeQuietly(cache2);
        }
    }

    @Test
    public void testDeleteNodeAfterCloseDoesntCallExecutor() throws Exception
    {
        client.create().forPath("/test");

        cache.start();
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test");
        assertEvent(TreeCacheEvent.Type.INITIALIZED);

        client.create().forPath("/test/one", "hey there".getBytes());
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/one");
        Assert.assertEquals(new String(cache.getCurrentData("/test/one").getData()), "hey there");

        cache.close();
        assertNoMoreEvents();

        client.delete().forPath("/test/one");
        assertNoMoreEvents();
    }

    private void assertNoMoreEvents() throws InterruptedException
    {
        timing.sleepABit();
        Assert.assertTrue(events.isEmpty());
    }

    private TreeCacheEvent assertEvent(TreeCacheEvent.Type expectedType) throws InterruptedException
    {
        return assertEvent(expectedType, null);
    }

    private TreeCacheEvent assertEvent(TreeCacheEvent.Type expectedType, String expectedPath) throws InterruptedException
    {
        return assertEvent(expectedType, expectedPath, null);
    }

    private TreeCacheEvent assertEvent(TreeCacheEvent.Type expectedType, String expectedPath, byte[] expectedData) throws InterruptedException
    {
        TreeCacheEvent event = events.poll(timing.forWaiting().seconds(), TimeUnit.SECONDS);
        Assert.assertEquals(event.getType(), expectedType, event.toString());
        if ( expectedPath == null )
        {
            Assert.assertNull(event.getData(), event.toString());
        }
        else
        {
            Assert.assertEquals(event.getData().getPath(), expectedPath, event.toString());
        }
        if ( expectedData != null )
        {
            Assert.assertEquals(event.getData().getData(), expectedData, event.toString());
        }
        return event;
    }
}
