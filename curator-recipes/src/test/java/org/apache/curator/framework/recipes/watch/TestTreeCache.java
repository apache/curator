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

import org.apache.curator.test.compatibility.KillSession2;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;

public class TestTreeCache extends BaseTestTreeCache
{
    @Test
    public void testSelector() throws Exception
    {
        client.create().forPath("/root");
        client.create().forPath("/root/n1-a");
        client.create().forPath("/root/n1-b");
        client.create().forPath("/root/n1-b/n2-a");
        client.create().forPath("/root/n1-b/n2-b");
        client.create().forPath("/root/n1-b/n2-b/n3-a");
        client.create().forPath("/root/n1-c");
        client.create().forPath("/root/n1-d");

        CacheSelector selector = new CacheSelector()
        {
            @Override
            public boolean traverseChildren(String basePath, String fullPath)
            {
                return !fullPath.equals("/root/n1-b/n2-b");
            }

            @Override
            public CacheAction actionForPath(String basePath, String fullPath)
            {
                return fullPath.equals("/root/n1-c") ? CacheAction.NOT_STORED : CacheAction.STAT_AND_DATA;
            }
        };
        cache = buildWithListeners(CuratorCacheBuilder.builder(client, "/root").withCacheSelector(selector));
        cache.start();

        assertEvent(CacheEvent.NODE_CREATED, "/root");
        assertEvent(CacheEvent.NODE_CREATED, "/root/n1-a");
        assertEvent(CacheEvent.NODE_CREATED, "/root/n1-b");
        assertEvent(CacheEvent.NODE_CREATED, "/root/n1-d");
        assertEvent(CacheEvent.NODE_CREATED, "/root/n1-b/n2-a");
        assertEvent(CacheEvent.NODE_CREATED, "/root/n1-b/n2-b");
        assertEvent(CacheEvent.CACHE_REFRESHED);
        assertNoMoreEvents();
    }

    @Test
    public void testStartup() throws Exception
    {
        client.create().forPath("/test");
        client.create().forPath("/test/1", "one".getBytes());
        client.create().forPath("/test/2", "two".getBytes());
        client.create().forPath("/test/3", "three".getBytes());
        client.create().forPath("/test/2/sub", "two-sub".getBytes());

        cache = newTreeCacheWithListeners(client, "/test");
        cache.start();
        assertEvent(CacheEvent.NODE_CREATED, "/test");
        assertEvent(CacheEvent.NODE_CREATED, "/test/1", "one".getBytes());
        assertEvent(CacheEvent.NODE_CREATED, "/test/2", "two".getBytes());
        assertEvent(CacheEvent.NODE_CREATED, "/test/3", "three".getBytes());
        assertEvent(CacheEvent.NODE_CREATED, "/test/2/sub", "two-sub".getBytes());
        assertEvent(CacheEvent.CACHE_REFRESHED);
        assertNoMoreEvents();

        assertChildNodeNames("/test", "1", "2", "3");
        assertChildNodeNames("/test/1");
        assertChildNodeNames("/test/2", "sub");
        Assert.assertNull(cache.get("/test/non_exist"));
    }

    @Test
    public void testStartEmpty() throws Exception
    {
        cache = newTreeCacheWithListeners(client, "/test");
        cache.start();
        assertEvent(CacheEvent.CACHE_REFRESHED);

        client.create().forPath("/test");
        assertEvent(CacheEvent.NODE_CREATED, "/test");
        assertNoMoreEvents();
    }

    @Test
    public void testStartEmptyDeeper() throws Exception
    {
        cache = newTreeCacheWithListeners(client, "/test/foo/bar");
        cache.start();
        assertEvent(CacheEvent.CACHE_REFRESHED);

        client.create().creatingParentsIfNeeded().forPath("/test/foo");
        assertNoMoreEvents();
        client.create().forPath("/test/foo/bar");
        assertEvent(CacheEvent.NODE_CREATED, "/test/foo/bar");
        assertNoMoreEvents();
    }

    @Test
    public void testDepth0() throws Exception
    {
        client.create().forPath("/test");
        client.create().forPath("/test/1", "one".getBytes());
        client.create().forPath("/test/2", "two".getBytes());
        client.create().forPath("/test/3", "three".getBytes());
        client.create().forPath("/test/2/sub", "two-sub".getBytes());

        cache = buildWithListeners(CuratorCacheBuilder.builder(client, "/test").withCacheSelector(CacheSelectors.maxDepth(0)));
        cache.start();
        assertEvent(CacheEvent.NODE_CREATED, "/test");
        assertEvent(CacheEvent.CACHE_REFRESHED);
        assertNoMoreEvents();

        assertChildNodeNames("/test");
        Assert.assertNull(cache.get("/test/1"));
        assertChildNodeNames("/test/1");
        Assert.assertNull(cache.get("/test/non_exist"));
    }

    @Test
    public void testDepth1() throws Exception
    {
        client.create().forPath("/test");
        client.create().forPath("/test/1", "one".getBytes());
        client.create().forPath("/test/2", "two".getBytes());
        client.create().forPath("/test/3", "three".getBytes());
        client.create().forPath("/test/2/sub", "two-sub".getBytes());

        cache = buildWithListeners(CuratorCacheBuilder.builder(client, "/test").withCacheSelector(CacheSelectors.maxDepth(1)));
        cache.start();
        assertEvent(CacheEvent.NODE_CREATED, "/test");
        assertEvent(CacheEvent.NODE_CREATED, "/test/1", "one".getBytes());
        assertEvent(CacheEvent.NODE_CREATED, "/test/2", "two".getBytes());
        assertEvent(CacheEvent.NODE_CREATED, "/test/3", "three".getBytes());
        assertEvent(CacheEvent.CACHE_REFRESHED);
        assertNoMoreEvents();

        assertChildNodeNames("/test", "1", "2", "3");
        assertChildNodeNames("/test/1");
        assertChildNodeNames("/test/2");
        Assert.assertNull(cache.get("/test/2/sub"));
        assertChildNodeNames("/test/2/sub");
        assertChildNodeNames("/test/non_exist");
    }

    @Test
    public void testDepth1Deeper() throws Exception
    {
        client.create().forPath("/test");
        client.create().forPath("/test/foo");
        client.create().forPath("/test/foo/bar");
        client.create().forPath("/test/foo/bar/1", "one".getBytes());
        client.create().forPath("/test/foo/bar/2", "two".getBytes());
        client.create().forPath("/test/foo/bar/3", "three".getBytes());
        client.create().forPath("/test/foo/bar/2/sub", "two-sub".getBytes());

        cache = buildWithListeners(CuratorCacheBuilder.builder(client, "/test/foo/bar").withCacheSelector(CacheSelectors.maxDepth((1))));
        cache.start();
        assertEvent(CacheEvent.NODE_CREATED, "/test/foo/bar");
        assertEvent(CacheEvent.NODE_CREATED, "/test/foo/bar/1", "one".getBytes());
        assertEvent(CacheEvent.NODE_CREATED, "/test/foo/bar/2", "two".getBytes());
        assertEvent(CacheEvent.NODE_CREATED, "/test/foo/bar/3", "three".getBytes());
        assertEvent(CacheEvent.CACHE_REFRESHED);
        assertNoMoreEvents();
    }

    @Test
    public void testAsyncInitialPopulation() throws Exception
    {
        client.create().forPath("/test");
        client.create().forPath("/test/one", "hey there".getBytes());

        cache = newTreeCacheWithListeners(client, "/test");
        cache.start();
        assertEvent(CacheEvent.NODE_CREATED, "/test");
        assertEvent(CacheEvent.NODE_CREATED, "/test/one");
        assertEvent(CacheEvent.CACHE_REFRESHED);
        assertNoMoreEvents();
    }

    @Test
    public void testFromRoot() throws Exception
    {
        client.create().forPath("/test");
        client.create().forPath("/test/one", "hey there".getBytes());

        cache = newTreeCacheWithListeners(client, "/");
        cache.start();
        assertEvent(CacheEvent.NODE_CREATED, "/");
        assertEvent(CacheEvent.NODE_CREATED, "/test");
        assertEvent(CacheEvent.NODE_CREATED, "/test/one");
        assertEvent(CacheEvent.CACHE_REFRESHED);
        assertNoMoreEvents();

        Assert.assertTrue(cache.childrenAtPath("/").keySet().contains("test"));
        assertChildNodeNames("/test", "one");
        assertChildNodeNames("/test/one");
        Assert.assertEquals(new String(cache.get("/test/one").getData()), "hey there");
    }

    @Test
    public void testFromRootWithDepth() throws Exception
    {
        client.create().forPath("/test");
        client.create().forPath("/test/one", "hey there".getBytes());

        cache = buildWithListeners(CuratorCacheBuilder.builder(client, "/").withCacheSelector(CacheSelectors.maxDepth((1))));
        cache.start();
        assertEvent(CacheEvent.NODE_CREATED, "/");
        assertEvent(CacheEvent.NODE_CREATED, "/test");
        assertEvent(CacheEvent.CACHE_REFRESHED);
        assertNoMoreEvents();

        Assert.assertTrue(cache.childrenAtPath("/").keySet().contains("test"));
        assertChildNodeNames("/test");
        Assert.assertNull(cache.get("/test/one"));
        assertChildNodeNames("/test/one");
    }

    @Test
    public void testWithNamespace() throws Exception
    {
        client.create().forPath("/outer");
        client.create().forPath("/outer/foo");
        client.create().forPath("/outer/test");
        client.create().forPath("/outer/test/one", "hey there".getBytes());

        cache = newTreeCacheWithListeners(client.usingNamespace("outer"), "/test");
        cache.start();
        assertEvent(CacheEvent.NODE_CREATED, "/test");
        assertEvent(CacheEvent.NODE_CREATED, "/test/one");
        assertEvent(CacheEvent.CACHE_REFRESHED);
        assertNoMoreEvents();

        assertChildNodeNames("/test", "one");
        assertChildNodeNames("/test/one");
        Assert.assertEquals(new String(cache.get("/test/one").getData()), "hey there");
    }

    @Test
    public void testWithNamespaceAtRoot() throws Exception
    {
        client.create().forPath("/outer");
        client.create().forPath("/outer/foo");
        client.create().forPath("/outer/test");
        client.create().forPath("/outer/test/one", "hey there".getBytes());

        cache = newTreeCacheWithListeners(client.usingNamespace("outer"), "/");
        cache.start();
        assertEvent(CacheEvent.NODE_CREATED, "/");
        assertEvent(CacheEvent.NODE_CREATED, "/foo");
        assertEvent(CacheEvent.NODE_CREATED, "/test");
        assertEvent(CacheEvent.NODE_CREATED, "/test/one");
        assertEvent(CacheEvent.CACHE_REFRESHED);
        assertNoMoreEvents();
        assertChildNodeNames("/", "foo", "test");
        assertChildNodeNames("/foo");
        assertChildNodeNames("/test", "one");
        assertChildNodeNames("/test/one");
        Assert.assertEquals(new String(cache.get("/test/one").getData()), "hey there");
    }

    @Test
    public void testSyncInitialPopulation() throws Exception
    {
        cache = newTreeCacheWithListeners(client, "/test");
        cache.start();
        assertEvent(CacheEvent.CACHE_REFRESHED);

        client.create().forPath("/test");
        client.create().forPath("/test/one", "hey there".getBytes());
        assertEvent(CacheEvent.NODE_CREATED, "/test");
        assertEvent(CacheEvent.NODE_CREATED, "/test/one");
        assertNoMoreEvents();
    }

    @Test
    public void testChildrenInitialized() throws Exception
    {
        client.create().forPath("/test", "".getBytes());
        client.create().forPath("/test/1", "1".getBytes());
        client.create().forPath("/test/2", "2".getBytes());
        client.create().forPath("/test/3", "3".getBytes());

        cache = newTreeCacheWithListeners(client, "/test");
        cache.start();
        assertEvent(CacheEvent.NODE_CREATED, "/test");
        assertEvent(CacheEvent.NODE_CREATED, "/test/1");
        assertEvent(CacheEvent.NODE_CREATED, "/test/2");
        assertEvent(CacheEvent.NODE_CREATED, "/test/3");
        assertEvent(CacheEvent.CACHE_REFRESHED);
        assertNoMoreEvents();
    }

    @Test
    public void testUpdateWhenNotCachingData() throws Exception
    {
        client.create().forPath("/test");

        cache = buildWithListeners(CuratorCacheBuilder.builder(client, "/test").withCacheSelector(CacheSelectors.statOnly()));
        cache.start();
        assertEvent(CacheEvent.NODE_CREATED, "/test");
        assertEvent(CacheEvent.CACHE_REFRESHED);

        client.create().forPath("/test/foo", "first".getBytes());
        assertEvent(CacheEvent.NODE_CREATED, "/test/foo");

        client.setData().forPath("/test/foo", "something new".getBytes());
        assertEvent(CacheEvent.NODE_CHANGED, "/test/foo");
        assertNoMoreEvents();

        Assert.assertNotNull(cache.get("/test/foo"));
        // No byte data querying the tree because we're not caching data.
        Assert.assertEquals(cache.get("/test/foo").getData().length, 0);
    }

    @Test
    public void testDeleteThenCreate() throws Exception
    {
        client.create().forPath("/test");
        client.create().forPath("/test/foo", "one".getBytes());

        cache = newTreeCacheWithListeners(client, "/test");
        cache.start();
        assertEvent(CacheEvent.NODE_CREATED, "/test");
        assertEvent(CacheEvent.NODE_CREATED, "/test/foo");
        assertEvent(CacheEvent.CACHE_REFRESHED);

        client.delete().forPath("/test/foo");
        assertEvent(CacheEvent.NODE_DELETED, "/test/foo", "one".getBytes());
        client.create().forPath("/test/foo", "two".getBytes());
        assertEvent(CacheEvent.NODE_CREATED, "/test/foo");

        client.delete().forPath("/test/foo");
        assertEvent(CacheEvent.NODE_DELETED, "/test/foo", "two".getBytes());
        client.create().forPath("/test/foo", "two".getBytes());
        assertEvent(CacheEvent.NODE_CREATED, "/test/foo");

        assertNoMoreEvents();
    }

    @Test
    public void testDeleteThenCreateRoot() throws Exception
    {
        client.create().forPath("/test");
        client.create().forPath("/test/foo", "one".getBytes());

        cache = newTreeCacheWithListeners(client, "/test/foo");
        cache.start();
        assertEvent(CacheEvent.NODE_CREATED, "/test/foo");
        assertEvent(CacheEvent.CACHE_REFRESHED);

        client.delete().forPath("/test/foo");
        assertEvent(CacheEvent.NODE_DELETED, "/test/foo");
        client.create().forPath("/test/foo", "two".getBytes());
        assertEvent(CacheEvent.NODE_CREATED, "/test/foo");

        client.delete().forPath("/test/foo");
        assertEvent(CacheEvent.NODE_DELETED, "/test/foo");
        client.create().forPath("/test/foo", "two".getBytes());
        assertEvent(CacheEvent.NODE_CREATED, "/test/foo");

        assertNoMoreEvents();
    }

    @Test
    public void testKilledSession() throws Exception
    {
        client.create().forPath("/test");

        cache = newTreeCacheWithListeners(client, "/test");
        cache.start();
        assertEvent(CacheEvent.NODE_CREATED, "/test");
        assertEvent(CacheEvent.CACHE_REFRESHED);

        client.create().forPath("/test/foo", "foo".getBytes());
        assertEvent(CacheEvent.NODE_CREATED, "/test/foo");
        client.create().withMode(CreateMode.EPHEMERAL).forPath("/test/me", "data".getBytes());
        assertEvent(CacheEvent.NODE_CREATED, "/test/me");

        KillSession2.kill(client.getZookeeperClient().getZooKeeper());
        assertEvent(CacheEvent.CACHE_REFRESHED);
        assertEvent(CacheEvent.NODE_DELETED, "/test/me", "data".getBytes());

        assertNoMoreEvents();
    }

    @Test
    public void testBasics() throws Exception
    {
        client.create().forPath("/test");

        cache = newTreeCacheWithListeners(client, "/test");
        cache.start();
        assertEvent(CacheEvent.NODE_CREATED, "/test");
        assertEvent(CacheEvent.CACHE_REFRESHED);
        assertChildNodeNames("/test");
        Assert.assertNull(cache.get("/t"));
        Assert.assertNull(cache.get("/testing"));

        client.create().forPath("/test/one", "hey there".getBytes());
        assertEvent(CacheEvent.NODE_CREATED, "/test/one");
        assertChildNodeNames("/test", "one");
        Assert.assertEquals(new String(cache.get("/test/one").getData()), "hey there");
        assertChildNodeNames("/test/one");
        Assert.assertNull(cache.get("/test/o"));
        Assert.assertNull(cache.get("/test/onely"));

        client.setData().forPath("/test/one", "sup!".getBytes());
        assertEvent(CacheEvent.NODE_CHANGED, "/test/one");
        assertChildNodeNames("/test", "one");
        Assert.assertEquals(new String(cache.get("/test/one").getData()), "sup!");

        client.delete().forPath("/test/one");
        assertEvent(CacheEvent.NODE_DELETED, "/test/one", "sup!".getBytes());
        assertChildNodeNames("/test");

        assertNoMoreEvents();
    }

    @Test
    public void testBasicsOnTwoCaches() throws Exception
    {
        CuratorCache cache2 = newTreeCacheWithListeners(client, "/test");
        cache2.getListenable().removeListener(eventListener);  // Don't listen on the second cache.

        // Just ensures the same event count; enables test flow control on cache2.
        final Semaphore semaphore = new Semaphore(0);
        cache2.getListenable().addListener(new CacheListener()
        {
            @Override
            public void process(CacheEvent event, String path, CachedNode affectedNode)
            {
                semaphore.release();
            }
        });

        try
        {
            client.create().forPath("/test");

            cache = newTreeCacheWithListeners(client, "/test");
            cache.start();
            cache2.start();

            assertEvent(CacheEvent.NODE_CREATED, "/test");
            assertEvent(CacheEvent.CACHE_REFRESHED);
            semaphore.acquire(2);

            client.create().forPath("/test/one", "hey there".getBytes());
            assertEvent(CacheEvent.NODE_CREATED, "/test/one");
            Assert.assertEquals(new String(cache.get("/test/one").getData()), "hey there");
            semaphore.acquire();
            Assert.assertEquals(new String(cache2.get("/test/one").getData()), "hey there");

            client.setData().forPath("/test/one", "sup!".getBytes());
            assertEvent(CacheEvent.NODE_CHANGED, "/test/one");
            Assert.assertEquals(new String(cache.get("/test/one").getData()), "sup!");
            semaphore.acquire();
            Assert.assertEquals(new String(cache2.get("/test/one").getData()), "sup!");

            client.delete().forPath("/test/one");
            assertEvent(CacheEvent.NODE_DELETED, "/test/one", "sup!".getBytes());
            Assert.assertNull(cache.get("/test/one"));
            semaphore.acquire();
            Assert.assertNull(cache2.get("/test/one"));

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

        cache = newTreeCacheWithListeners(client, "/test");
        cache.start();
        assertEvent(CacheEvent.NODE_CREATED, "/test");
        assertEvent(CacheEvent.CACHE_REFRESHED);

        client.create().forPath("/test/one", "hey there".getBytes());
        assertEvent(CacheEvent.NODE_CREATED, "/test/one");
        Assert.assertEquals(new String(cache.get("/test/one").getData()), "hey there");

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
        cache = newTreeCacheWithListeners(client, "/test");
        cache.start();
        assertNoMoreEvents();

        // Now restart the server.
        server.restart();
        assertEvent(CacheEvent.CACHE_REFRESHED);

        client.create().forPath("/test");

        assertEvent(CacheEvent.NODE_CREATED, "/test");
        assertNoMoreEvents();
    }

    @Test
    public void testCompositeSelector() throws Exception
    {
        client.create().creatingParentsIfNeeded().forPath("/root/a1/b1/c1");
        client.create().creatingParentsIfNeeded().forPath("/root/a1/b1/c2");
        client.create().creatingParentsIfNeeded().forPath("/root/a1/b2/c1");

        client.create().creatingParentsIfNeeded().forPath("/root/a2/b1/c1");
        client.create().creatingParentsIfNeeded().forPath("/root/a2/b1/c2");
        client.create().creatingParentsIfNeeded().forPath("/root/a2/b2/c1");

        client.create().creatingParentsIfNeeded().forPath("/root/a3/b1/c1");
        client.create().creatingParentsIfNeeded().forPath("/root/a3/b1/c2");
        client.create().creatingParentsIfNeeded().forPath("/root/a3/b2/c1");

        CacheSelector depth0 = CacheSelectors.maxDepth(0);
        CacheSelector depth1 = CacheSelectors.maxDepth(1);
        CacheSelector standard = CacheSelectors.statAndData();
        Map<String, CacheSelector> map = new HashMap<>();
        map.put("/root/a1", depth0);
        map.put("/root/a2", depth1);
        map.put("/root/a3", standard);
        map.put("/root/a3/b2", depth0);
        CacheSelector composite = CacheSelectors.composite(map);
        cache = CuratorCacheBuilder.builder(client, "/root").withCacheSelector(composite).build();
        Assert.assertTrue(timing.awaitLatch(cache.start()));

        Assert.assertTrue(cache.exists("/root"));

        Assert.assertFalse(cache.exists("/root/a1"));
        Assert.assertFalse(cache.exists("/root/a1/b1"));
        Assert.assertFalse(cache.exists("/root/a1/b2"));

        Assert.assertTrue(cache.exists("/root/a2"));
        Assert.assertFalse(cache.exists("/root/a2/b1"));
        Assert.assertFalse(cache.exists("/root/a2/b2"));
        Assert.assertFalse(cache.exists("/root/a2/b1/c1"));
        Assert.assertFalse(cache.exists("/root/a2/b1/c2"));
        Assert.assertFalse(cache.exists("/root/a2/b2/c1"));

        Assert.assertTrue(cache.exists("/root/a3"));
        Assert.assertTrue(cache.exists("/root/a3/b1"));
        Assert.assertFalse(cache.exists("/root/a3/b2"));
        Assert.assertTrue(cache.exists("/root/a3/b1/c1"));
        Assert.assertTrue(cache.exists("/root/a3/b1/c2"));
        Assert.assertFalse(cache.exists("/root/a3/b2/c1"));
    }
}
