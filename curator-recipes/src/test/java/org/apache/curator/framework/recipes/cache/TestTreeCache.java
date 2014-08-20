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

import com.google.common.collect.ImmutableSet;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.KillSession;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.concurrent.Semaphore;

public class TestTreeCache extends BaseTestTreeCache
{
    @Test
    public void testStartup() throws Exception
    {
        client.create().forPath("/test");
        client.create().forPath("/test/1", "one".getBytes());
        client.create().forPath("/test/2", "two".getBytes());
        client.create().forPath("/test/3", "three".getBytes());
        client.create().forPath("/test/2/sub", "two-sub".getBytes());

        cache = new MyTreeCache(client, "/test", true);
        cache.start();
        assertEvent(CacheEvent.Type.NODE_ADDED, "/test");
        assertEvent(CacheEvent.Type.NODE_ADDED, "/test/1", "one".getBytes());
        assertEvent(CacheEvent.Type.NODE_ADDED, "/test/2", "two".getBytes());
        assertEvent(CacheEvent.Type.NODE_ADDED, "/test/3", "three".getBytes());
        assertEvent(CacheEvent.Type.NODE_ADDED, "/test/2/sub", "two-sub".getBytes());
        assertEvent(CacheEvent.Type.INITIALIZED);
        assertNoMoreEvents();

        Assert.assertEquals(cache.getCurrentChildren("/test").keySet(), ImmutableSet.of("1", "2", "3"));
        Assert.assertEquals(cache.getCurrentChildren("/test/1").keySet(), ImmutableSet.of());
        Assert.assertEquals(cache.getCurrentChildren("/test/2").keySet(), ImmutableSet.of("sub"));
        Assert.assertNull(cache.getCurrentChildren("/test/non_exist"));
    }

    @Test
    public void testStartEmpty() throws Exception
    {
        cache = new MyTreeCache(client, "/test", true);
        cache.start();
        assertEvent(CacheEvent.Type.INITIALIZED);

        client.create().forPath("/test");
        assertEvent(CacheEvent.Type.NODE_ADDED, "/test");
        assertNoMoreEvents();
    }

    @Test
    public void testAsyncInitialPopulation() throws Exception
    {
        client.create().forPath("/test");
        client.create().forPath("/test/one", "hey there".getBytes());

        cache = new MyTreeCache(client, "/test", true);
        cache.start();
        assertEvent(CacheEvent.Type.NODE_ADDED, "/test");
        assertEvent(CacheEvent.Type.NODE_ADDED, "/test/one");
        assertEvent(CacheEvent.Type.INITIALIZED);
        assertNoMoreEvents();
    }

    @Test
    public void testFromRoot() throws Exception
    {
        client.create().forPath("/test");
        client.create().forPath("/test/one", "hey there".getBytes());

        cache = new MyTreeCache(client, "/", true);
        cache.start();
        assertEvent(CacheEvent.Type.NODE_ADDED, "/");
        assertEvent(CacheEvent.Type.NODE_ADDED, "/test");
        assertEvent(CacheEvent.Type.NODE_ADDED, "/test/one");
        assertEvent(CacheEvent.Type.INITIALIZED);
        assertNoMoreEvents();

        Assert.assertTrue(cache.getCurrentChildren("/").keySet().contains("test"));
        Assert.assertEquals(cache.getCurrentChildren("/test").keySet(), ImmutableSet.of("one"));
        Assert.assertEquals(cache.getCurrentChildren("/test/one").keySet(), ImmutableSet.of());
        Assert.assertEquals(new String(cache.getCurrentData("/test/one").getData()), "hey there");
    }

    @Test
    public void testWithNamespace() throws Exception
    {
        client.create().forPath("/outer");
        client.create().forPath("/outer/foo");
        client.create().forPath("/outer/test");
        client.create().forPath("/outer/test/one", "hey there".getBytes());

        cache = new MyTreeCache(client.usingNamespace("outer"), "/test", true);
        cache.start();
        assertEvent(CacheEvent.Type.NODE_ADDED, "/test");
        assertEvent(CacheEvent.Type.NODE_ADDED, "/test/one");
        assertEvent(CacheEvent.Type.INITIALIZED);
        assertNoMoreEvents();

        Assert.assertEquals(cache.getCurrentChildren("/test").keySet(), ImmutableSet.of("one"));
        Assert.assertEquals(cache.getCurrentChildren("/test/one").keySet(), ImmutableSet.of());
        Assert.assertEquals(new String(cache.getCurrentData("/test/one").getData()), "hey there");
    }

    @Test
    public void testWithNamespaceAtRoot() throws Exception
    {
        client.create().forPath("/outer");
        client.create().forPath("/outer/foo");
        client.create().forPath("/outer/test");
        client.create().forPath("/outer/test/one", "hey there".getBytes());

        cache = new MyTreeCache(client.usingNamespace("outer"), "/", true);
        cache.start();
        assertEvent(CacheEvent.Type.NODE_ADDED, "/");
        assertEvent(CacheEvent.Type.NODE_ADDED, "/foo");
        assertEvent(CacheEvent.Type.NODE_ADDED, "/test");
        assertEvent(CacheEvent.Type.NODE_ADDED, "/test/one");
        assertEvent(CacheEvent.Type.INITIALIZED);
        assertNoMoreEvents();
        Assert.assertEquals(cache.getCurrentChildren("/").keySet(), ImmutableSet.of("foo", "test"));
        Assert.assertEquals(cache.getCurrentChildren("/foo").keySet(), ImmutableSet.of());
        Assert.assertEquals(cache.getCurrentChildren("/test").keySet(), ImmutableSet.of("one"));
        Assert.assertEquals(cache.getCurrentChildren("/test/one").keySet(), ImmutableSet.of());
        Assert.assertEquals(new String(cache.getCurrentData("/test/one").getData()), "hey there");
    }

    @Test
    public void testSyncInitialPopulation() throws Exception
    {
        cache = new MyTreeCache(client, "/test", true);
        cache.start();
        assertEvent(CacheEvent.Type.INITIALIZED);

        client.create().forPath("/test");
        client.create().forPath("/test/one", "hey there".getBytes());
        assertEvent(CacheEvent.Type.NODE_ADDED, "/test");
        assertEvent(CacheEvent.Type.NODE_ADDED, "/test/one");
        assertNoMoreEvents();
    }

    @Test
    public void testChildrenInitialized() throws Exception
    {
        client.create().forPath("/test", "".getBytes());
        client.create().forPath("/test/1", "1".getBytes());
        client.create().forPath("/test/2", "2".getBytes());
        client.create().forPath("/test/3", "3".getBytes());

        cache = new MyTreeCache(client, "/test", true);
        cache.start();
        assertEvent(CacheEvent.Type.NODE_ADDED, "/test");
        assertEvent(CacheEvent.Type.NODE_ADDED, "/test/1");
        assertEvent(CacheEvent.Type.NODE_ADDED, "/test/2");
        assertEvent(CacheEvent.Type.NODE_ADDED, "/test/3");
        assertEvent(CacheEvent.Type.INITIALIZED);
        assertNoMoreEvents();
    }

    @Test
    public void testUpdateWhenNotCachingData() throws Exception
    {
        client.create().forPath("/test");

        cache = new MyTreeCache(client, "/test", false);
        cache.start();
        assertEvent(CacheEvent.Type.NODE_ADDED, "/test");
        assertEvent(CacheEvent.Type.INITIALIZED);

        client.create().forPath("/test/foo", "first".getBytes());
        assertEvent(CacheEvent.Type.NODE_ADDED, "/test/foo");

        client.setData().forPath("/test/foo", "something new".getBytes());
        assertEvent(CacheEvent.Type.NODE_UPDATED, "/test/foo");
        assertNoMoreEvents();
    }

    @Test
    public void testDeleteThenCreate() throws Exception
    {
        client.create().forPath("/test");
        client.create().forPath("/test/foo", "one".getBytes());

        cache = new MyTreeCache(client, "/test", true);
        cache.start();
        assertEvent(CacheEvent.Type.NODE_ADDED, "/test");
        assertEvent(CacheEvent.Type.NODE_ADDED, "/test/foo");
        assertEvent(CacheEvent.Type.INITIALIZED);

        client.delete().forPath("/test/foo");
        assertEvent(CacheEvent.Type.NODE_REMOVED, "/test/foo");
        client.create().forPath("/test/foo", "two".getBytes());
        assertEvent(CacheEvent.Type.NODE_ADDED, "/test/foo");

        assertNoMoreEvents();
    }

    @Test
    public void testKilledSession() throws Exception
    {
        client.create().forPath("/test");

        cache = new MyTreeCache(client, "/test", true);
        cache.start();
        assertEvent(CacheEvent.Type.NODE_ADDED, "/test");
        assertEvent(CacheEvent.Type.INITIALIZED);

        client.create().forPath("/test/foo", "foo".getBytes());
        assertEvent(CacheEvent.Type.NODE_ADDED, "/test/foo");
        client.create().withMode(CreateMode.EPHEMERAL).forPath("/test/me", "data".getBytes());
        assertEvent(CacheEvent.Type.NODE_ADDED, "/test/me");

        KillSession.kill(client.getZookeeperClient().getZooKeeper(), server.getConnectString());
        assertEvent(CacheEvent.Type.CONNECTION_SUSPENDED);
        assertEvent(CacheEvent.Type.CONNECTION_LOST);
        assertEvent(CacheEvent.Type.CONNECTION_RECONNECTED);
        assertEvent(CacheEvent.Type.NODE_REMOVED, "/test/me");

        assertNoMoreEvents();
    }

    @Test
    public void testBasics() throws Exception
    {
        client.create().forPath("/test");

        cache = new MyTreeCache(client, "/test", true);
        cache.start();
        assertEvent(CacheEvent.Type.NODE_ADDED, "/test");
        assertEvent(CacheEvent.Type.INITIALIZED);
        Assert.assertEquals(cache.getCurrentChildren("/test").keySet(), ImmutableSet.of());

        client.create().forPath("/test/one", "hey there".getBytes());
        assertEvent(CacheEvent.Type.NODE_ADDED, "/test/one");
        Assert.assertEquals(cache.getCurrentChildren("/test").keySet(), ImmutableSet.of("one"));
        Assert.assertEquals(new String(cache.getCurrentData("/test/one").getData()), "hey there");

        client.setData().forPath("/test/one", "sup!".getBytes());
        assertEvent(CacheEvent.Type.NODE_UPDATED, "/test/one");
        Assert.assertEquals(cache.getCurrentChildren("/test").keySet(), ImmutableSet.of("one"));
        Assert.assertEquals(new String(cache.getCurrentData("/test/one").getData()), "sup!");

        client.delete().forPath("/test/one");
        assertEvent(CacheEvent.Type.NODE_REMOVED, "/test/one");
        Assert.assertEquals(cache.getCurrentChildren("/test").keySet(), ImmutableSet.of());

        assertNoMoreEvents();
    }

    @Test
    public void testBasicsOnTwoCaches() throws Exception
    {
        MyTreeCache cache2 = new MyTreeCache(client, "/test", true);
        cache2.getListenable().removeListener(eventListener);  // Don't listen on the second cache.

        // Just ensures the same event count; enables test flow control on cache2.
        final Semaphore semaphore = new Semaphore(0);
        cache2.getListenable().addListener(new CacheListener()
        {
            @Override
            public void childEvent(CuratorFramework client, CacheEvent event) throws Exception
            {
                semaphore.release();
            }
        });

        try
        {
            client.create().forPath("/test");

            cache = new MyTreeCache(client, "/test", true);
            cache.start();
            cache2.start();

            assertEvent(CacheEvent.Type.NODE_ADDED, "/test");
            assertEvent(CacheEvent.Type.INITIALIZED);
            semaphore.acquire(2);

            client.create().forPath("/test/one", "hey there".getBytes());
            assertEvent(CacheEvent.Type.NODE_ADDED, "/test/one");
            Assert.assertEquals(new String(cache.getCurrentData("/test/one").getData()), "hey there");
            semaphore.acquire();
            Assert.assertEquals(new String(cache2.getCurrentData("/test/one").getData()), "hey there");

            client.setData().forPath("/test/one", "sup!".getBytes());
            assertEvent(CacheEvent.Type.NODE_UPDATED, "/test/one");
            Assert.assertEquals(new String(cache.getCurrentData("/test/one").getData()), "sup!");
            semaphore.acquire();
            Assert.assertEquals(new String(cache2.getCurrentData("/test/one").getData()), "sup!");

            client.delete().forPath("/test/one");
            assertEvent(CacheEvent.Type.NODE_REMOVED, "/test/one");
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

        cache = new MyTreeCache(client, "/test", true);
        cache.start();
        assertEvent(CacheEvent.Type.NODE_ADDED, "/test");
        assertEvent(CacheEvent.Type.INITIALIZED);

        client.create().forPath("/test/one", "hey there".getBytes());
        assertEvent(CacheEvent.Type.NODE_ADDED, "/test/one");
        Assert.assertEquals(new String(cache.getCurrentData("/test/one").getData()), "hey there");

        cache.close();
        assertNoMoreEvents();

        client.delete().forPath("/test/one");
        assertNoMoreEvents();
    }

    /**
     * Make sure TreeCache gets to a sane state when we can't initially connect to server.
     */
    @Test
    public void testServerNotStartedYet() throws Exception
    {
        // Stop the existing server.
        server.stop();

        // Shutdown the existing client and re-create it started.
        client.close();
        initCuratorFramework();

        // Start the client disconnected.
        cache = new MyTreeCache(client, "/test", true);
        cache.start();
        assertNoMoreEvents();

        // Now restart the server.
        server.restart();
        assertEvent(CacheEvent.Type.INITIALIZED);

        client.create().forPath("/test");

        assertEvent(CacheEvent.Type.NODE_ADDED, "/test");
        assertNoMoreEvents();
    }
}
