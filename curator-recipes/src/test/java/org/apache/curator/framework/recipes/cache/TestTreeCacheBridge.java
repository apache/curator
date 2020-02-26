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
import org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type;
import org.apache.curator.test.compatibility.KillSession2;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.concurrent.Semaphore;

@SuppressWarnings("deprecation")
public class TestTreeCacheBridge extends BaseTestTreeCache<TreeCacheBridge>
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

        TreeCacheSelector selector = new TreeCacheSelector()
        {
            @Override
            public boolean traverseChildren(String fullPath)
            {
                return !fullPath.equals("/root/n1-b/n2-b");
            }

            @Override
            public boolean acceptChild(String fullPath)
            {
                return !fullPath.equals("/root/n1-c");
            }
        };
        cache = buildBridgeWithListeners(TreeCache.newBuilder(client, "/root").setSelector(selector));
        cache.start();

        assertEvent(Type.NODE_ADDED, "/root");
        assertEvent(Type.NODE_ADDED, "/root/n1-a");
        assertEvent(Type.NODE_ADDED, "/root/n1-b");
        assertEvent(Type.NODE_ADDED, "/root/n1-d");
        assertEvent(Type.NODE_ADDED, "/root/n1-b/n2-a");
        assertEvent(Type.NODE_ADDED, "/root/n1-b/n2-b");
        assertEvent(Type.INITIALIZED);
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

        cache = newTreeCacheBridgeWithListeners(client, "/test");
        cache.start();
        assertEvent(Type.NODE_ADDED, "/test");
        assertEvent(Type.NODE_ADDED, "/test/1", "one".getBytes());
        assertEvent(Type.NODE_ADDED, "/test/2", "two".getBytes());
        assertEvent(Type.NODE_ADDED, "/test/3", "three".getBytes());
        assertEvent(Type.NODE_ADDED, "/test/2/sub", "two-sub".getBytes());
        assertEvent(Type.INITIALIZED);
        assertNoMoreEvents();

        Assert.assertEquals(cache.getCurrentChildren("/test").keySet(), ImmutableSet.of("1", "2", "3"));
        Assert.assertEquals(cache.getCurrentChildren("/test/1").keySet(), ImmutableSet.of());
        Assert.assertEquals(cache.getCurrentChildren("/test/2").keySet(), ImmutableSet.of("sub"));
        Assert.assertTrue(cache.getCurrentChildren("/test/non_exist").isEmpty());
    }

    @Test
    public void testStartEmpty() throws Exception
    {
        cache = newTreeCacheBridgeWithListeners(client, "/test");
        cache.start();
        assertEvent(Type.INITIALIZED);

        client.create().forPath("/test");
        assertEvent(Type.NODE_ADDED, "/test");
        assertNoMoreEvents();
    }

    @Test
    public void testStartEmptyDeeper() throws Exception
    {
        cache = newTreeCacheBridgeWithListeners(client, "/test/foo/bar");
        cache.start();
        assertEvent(Type.INITIALIZED);

        client.create().creatingParentsIfNeeded().forPath("/test/foo");
        assertNoMoreEvents();
        client.create().forPath("/test/foo/bar");
        assertEvent(Type.NODE_ADDED, "/test/foo/bar");
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

        cache = buildBridgeWithListeners(TreeCache.newBuilder(client, "/test").setMaxDepth(0));
        cache.start();
        assertEvent(Type.NODE_ADDED, "/test");
        assertEvent(Type.INITIALIZED);
        assertNoMoreEvents();

        Assert.assertEquals(cache.getCurrentChildren("/test").keySet(), ImmutableSet.of());
        Assert.assertNull(cache.getCurrentData("/test/1"));
        Assert.assertTrue(cache.getCurrentChildren("/test/1").isEmpty());
        Assert.assertNull(cache.getCurrentData("/test/non_exist"));
    }

    @Test
    public void testDepth1() throws Exception
    {
        client.create().forPath("/test");
        client.create().forPath("/test/1", "one".getBytes());
        client.create().forPath("/test/2", "two".getBytes());
        client.create().forPath("/test/3", "three".getBytes());
        client.create().forPath("/test/2/sub", "two-sub".getBytes());

        cache = buildBridgeWithListeners(TreeCache.newBuilder(client, "/test").setMaxDepth(1));
        cache.start();
        assertEvent(Type.NODE_ADDED, "/test");
        assertEvent(Type.NODE_ADDED, "/test/1", "one".getBytes());
        assertEvent(Type.NODE_ADDED, "/test/2", "two".getBytes());
        assertEvent(Type.NODE_ADDED, "/test/3", "three".getBytes());
        assertEvent(Type.INITIALIZED);
        assertNoMoreEvents();

        Assert.assertEquals(cache.getCurrentChildren("/test").keySet(), ImmutableSet.of("1", "2", "3"));
        Assert.assertEquals(cache.getCurrentChildren("/test/1").keySet(), ImmutableSet.of());
        Assert.assertEquals(cache.getCurrentChildren("/test/2").keySet(), ImmutableSet.of());
        Assert.assertNull(cache.getCurrentData("/test/2/sub"));
        Assert.assertTrue(cache.getCurrentChildren("/test/2/sub").isEmpty());
        Assert.assertTrue(cache.getCurrentChildren("/test/non_exist").isEmpty());
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

        cache = buildBridgeWithListeners(TreeCache.newBuilder(client, "/test/foo/bar").setMaxDepth(1));
        cache.start();
        assertEvent(Type.NODE_ADDED, "/test/foo/bar");
        assertEvent(Type.NODE_ADDED, "/test/foo/bar/1", "one".getBytes());
        assertEvent(Type.NODE_ADDED, "/test/foo/bar/2", "two".getBytes());
        assertEvent(Type.NODE_ADDED, "/test/foo/bar/3", "three".getBytes());
        assertEvent(Type.INITIALIZED);
        assertNoMoreEvents();
    }

    @Test
    public void testAsyncInitialPopulation() throws Exception
    {
        client.create().forPath("/test");
        client.create().forPath("/test/one", "hey there".getBytes());

        cache = newTreeCacheBridgeWithListeners(client, "/test");
        cache.start();
        assertEvent(Type.NODE_ADDED, "/test");
        assertEvent(Type.NODE_ADDED, "/test/one");
        assertEvent(Type.INITIALIZED);
        assertNoMoreEvents();
    }

    @Test
    public void testFromRoot() throws Exception
    {
        client.create().forPath("/test");
        client.create().forPath("/test/one", "hey there".getBytes());

        cache = newTreeCacheBridgeWithListeners(client, "/");
        cache.start();
        assertEvent(Type.NODE_ADDED, "/");
        assertEvent(Type.NODE_ADDED, "/test");
        assertEvent(Type.NODE_ADDED, "/test/one");
        assertEvent(Type.INITIALIZED);
        assertNoMoreEvents();

        Assert.assertTrue(cache.getCurrentChildren("/").keySet().contains("test"));
        Assert.assertEquals(cache.getCurrentChildren("/test").keySet(), ImmutableSet.of("one"));
        Assert.assertEquals(cache.getCurrentChildren("/test/one").keySet(), ImmutableSet.of());
        Assert.assertEquals(new String(cache.getCurrentData("/test/one").getData()), "hey there");
    }

    @Test
    public void testFromRootWithDepth() throws Exception
    {
        client.create().forPath("/test");
        client.create().forPath("/test/one", "hey there".getBytes());

        cache = buildBridgeWithListeners(TreeCache.newBuilder(client, "/").setMaxDepth(1));
        cache.start();
        assertEvent(Type.NODE_ADDED, "/");
        assertEvent(Type.NODE_ADDED, "/test");
        assertEvent(Type.INITIALIZED);
        assertNoMoreEvents();

        Assert.assertTrue(cache.getCurrentChildren("/").keySet().contains("test"));
        Assert.assertEquals(cache.getCurrentChildren("/test").keySet(), ImmutableSet.of());
        Assert.assertNull(cache.getCurrentData("/test/one"));
        Assert.assertTrue(cache.getCurrentChildren("/test/one").isEmpty());
    }

    @Test
    public void testWithNamespace() throws Exception
    {
        client.create().forPath("/outer");
        client.create().forPath("/outer/foo");
        client.create().forPath("/outer/test");
        client.create().forPath("/outer/test/one", "hey there".getBytes());

        cache = newTreeCacheBridgeWithListeners(client.usingNamespace("outer"), "/test");
        cache.start();
        assertEvent(Type.NODE_ADDED, "/test");
        assertEvent(Type.NODE_ADDED, "/test/one");
        assertEvent(Type.INITIALIZED);
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

        cache = newTreeCacheBridgeWithListeners(client.usingNamespace("outer"), "/");
        cache.start();
        assertEvent(Type.NODE_ADDED, "/");
        assertEvent(Type.NODE_ADDED, "/foo");
        assertEvent(Type.NODE_ADDED, "/test");
        assertEvent(Type.NODE_ADDED, "/test/one");
        assertEvent(Type.INITIALIZED);
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
        cache = newTreeCacheBridgeWithListeners(client, "/test");
        cache.start();
        assertEvent(Type.INITIALIZED);

        client.create().forPath("/test");
        client.create().forPath("/test/one", "hey there".getBytes());
        assertEvent(Type.NODE_ADDED, "/test");
        assertEvent(Type.NODE_ADDED, "/test/one");
        assertNoMoreEvents();
    }

    @Test
    public void testChildrenInitialized() throws Exception
    {
        client.create().forPath("/test", "".getBytes());
        client.create().forPath("/test/1", "1".getBytes());
        client.create().forPath("/test/2", "2".getBytes());
        client.create().forPath("/test/3", "3".getBytes());

        cache = newTreeCacheBridgeWithListeners(client, "/test");
        cache.start();
        assertEvent(Type.NODE_ADDED, "/test");
        assertEvent(Type.NODE_ADDED, "/test/1");
        assertEvent(Type.NODE_ADDED, "/test/2");
        assertEvent(Type.NODE_ADDED, "/test/3");
        assertEvent(Type.INITIALIZED);
        assertNoMoreEvents();
    }

    @Test
    public void testUpdateWhenNotCachingData() throws Exception
    {
        client.create().forPath("/test");

        cache = buildBridgeWithListeners(TreeCache.newBuilder(client, "/test").setCacheData(false));
        cache.start();
        assertEvent(Type.NODE_ADDED, "/test");
        assertEvent(Type.INITIALIZED);

        client.create().forPath("/test/foo", "first".getBytes());
        assertEvent(Type.NODE_ADDED, "/test/foo");

        client.setData().forPath("/test/foo", "something new".getBytes());
        assertEvent(Type.NODE_UPDATED, "/test/foo");
        assertNoMoreEvents();

        Assert.assertNotNull(cache.getCurrentData("/test/foo"));
        // No byte data querying the tree because we're not caching data.
        Assert.assertNull(cache.getCurrentData("/test/foo").getData());
    }

    @Test
    public void testDeleteThenCreate() throws Exception
    {
        client.create().forPath("/test");
        client.create().forPath("/test/foo", "one".getBytes());

        cache = newTreeCacheBridgeWithListeners(client, "/test");
        cache.start();
        assertEvent(Type.NODE_ADDED, "/test");
        assertEvent(Type.NODE_ADDED, "/test/foo");
        assertEvent(Type.INITIALIZED);

        client.delete().forPath("/test/foo");
        assertEvent(Type.NODE_REMOVED, "/test/foo", "one".getBytes());
        client.create().forPath("/test/foo", "two".getBytes());
        assertEvent(Type.NODE_ADDED, "/test/foo");

        client.delete().forPath("/test/foo");
        assertEvent(Type.NODE_REMOVED, "/test/foo", "two".getBytes());
        client.create().forPath("/test/foo", "two".getBytes());
        assertEvent(Type.NODE_ADDED, "/test/foo");

        assertNoMoreEvents();
    }

    @Test
    public void testDeleteThenCreateRoot() throws Exception
    {
        client.create().forPath("/test");
        client.create().forPath("/test/foo", "one".getBytes());

        cache = newTreeCacheBridgeWithListeners(client, "/test/foo");
        cache.start();
        assertEvent(Type.NODE_ADDED, "/test/foo");
        assertEvent(Type.INITIALIZED);

        client.delete().forPath("/test/foo");
        assertEvent(Type.NODE_REMOVED, "/test/foo");
        client.create().forPath("/test/foo", "two".getBytes());
        assertEvent(Type.NODE_ADDED, "/test/foo");

        client.delete().forPath("/test/foo");
        assertEvent(Type.NODE_REMOVED, "/test/foo");
        client.create().forPath("/test/foo", "two".getBytes());
        assertEvent(Type.NODE_ADDED, "/test/foo");

        assertNoMoreEvents();
    }

    @Test
    public void testKilledSession() throws Exception
    {
        client.create().forPath("/test");

        cache = newTreeCacheBridgeWithListeners(client, "/test");
        cache.start();
        assertEvent(Type.NODE_ADDED, "/test");
        assertEvent(Type.INITIALIZED);

        client.create().forPath("/test/foo", "foo".getBytes());
        assertEvent(Type.NODE_ADDED, "/test/foo");
        client.create().withMode(CreateMode.EPHEMERAL).forPath("/test/me", "data".getBytes());
        assertEvent(Type.NODE_ADDED, "/test/me");

        KillSession2.kill(client.getZookeeperClient().getZooKeeper());
        assertEvent(Type.CONNECTION_LOST);
        assertEvent(Type.CONNECTION_RECONNECTED);
        assertEvent(Type.INITIALIZED);
        assertEvent(Type.NODE_REMOVED, "/test/me", "data".getBytes());

        assertNoMoreEvents();
    }

    @Test
    public void testBasics() throws Exception
    {
        client.create().forPath("/test");

        cache = newTreeCacheBridgeWithListeners(client, "/test");
        cache.start();
        assertEvent(Type.NODE_ADDED, "/test");
        assertEvent(Type.INITIALIZED);
        Assert.assertEquals(cache.getCurrentChildren("/test").keySet(), ImmutableSet.of());
        Assert.assertTrue(cache.getCurrentChildren("/t").isEmpty());
        Assert.assertTrue(cache.getCurrentChildren("/testing").isEmpty());

        client.create().forPath("/test/one", "hey there".getBytes());
        assertEvent(Type.NODE_ADDED, "/test/one");
        Assert.assertEquals(cache.getCurrentChildren("/test").keySet(), ImmutableSet.of("one"));
        Assert.assertEquals(new String(cache.getCurrentData("/test/one").getData()), "hey there");
        Assert.assertEquals(cache.getCurrentChildren("/test/one").keySet(), ImmutableSet.of());
        Assert.assertTrue(cache.getCurrentChildren("/test/o").isEmpty());
        Assert.assertTrue(cache.getCurrentChildren("/test/onely").isEmpty());

        client.setData().forPath("/test/one", "sup!".getBytes());
        assertEvent(Type.NODE_UPDATED, "/test/one");
        Assert.assertEquals(cache.getCurrentChildren("/test").keySet(), ImmutableSet.of("one"));
        Assert.assertEquals(new String(cache.getCurrentData("/test/one").getData()), "sup!");

        client.delete().forPath("/test/one");
        assertEvent(Type.NODE_REMOVED, "/test/one", "sup!".getBytes());
        Assert.assertEquals(cache.getCurrentChildren("/test").keySet(), ImmutableSet.of());

        assertNoMoreEvents();
    }

    @Test
    public void testBasicsOnTwoCaches() throws Exception
    {
        TreeCacheBridge cache2 = newTreeCacheBridgeWithListeners(client, "/test");
        cache2.getListenable().removeListener(eventListener);  // Don't listen on the second cache.

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

            cache = newTreeCacheBridgeWithListeners(client, "/test");
            cache.start();
            cache2.start();

            assertEvent(Type.NODE_ADDED, "/test");
            assertEvent(Type.INITIALIZED);
            semaphore.acquire(2);

            client.create().forPath("/test/one", "hey there".getBytes());
            assertEvent(Type.NODE_ADDED, "/test/one");
            Assert.assertEquals(new String(cache.getCurrentData("/test/one").getData()), "hey there");
            semaphore.acquire();
            Assert.assertEquals(new String(cache2.getCurrentData("/test/one").getData()), "hey there");

            client.setData().forPath("/test/one", "sup!".getBytes());
            assertEvent(Type.NODE_UPDATED, "/test/one");
            Assert.assertEquals(new String(cache.getCurrentData("/test/one").getData()), "sup!");
            semaphore.acquire();
            Assert.assertEquals(new String(cache2.getCurrentData("/test/one").getData()), "sup!");

            client.delete().forPath("/test/one");
            assertEvent(Type.NODE_REMOVED, "/test/one", "sup!".getBytes());
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

        cache = newTreeCacheBridgeWithListeners(client, "/test");
        cache.start();
        assertEvent(Type.NODE_ADDED, "/test");
        assertEvent(Type.INITIALIZED);

        client.create().forPath("/test/one", "hey there".getBytes());
        assertEvent(Type.NODE_ADDED, "/test/one");
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
        cache = newTreeCacheBridgeWithListeners(client, "/test");
        cache.start();
        assertNoMoreEvents();

        // Now restart the server.
        server.restart();
        assertEvent(Type.INITIALIZED);

        client.create().forPath("/test");

        assertEvent(Type.NODE_ADDED, "/test");
        assertNoMoreEvents();
    }
}
