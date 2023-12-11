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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.google.common.collect.ImmutableSet;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type;
import org.apache.curator.test.compatibility.CuratorTestBase;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(CuratorTestBase.zk35TestCompatibilityGroup)
public class TestTreeCache extends BaseTestTreeCache {
    @Test
    public void testSelector() throws Exception {
        client.create().forPath("/root");
        client.create().forPath("/root/n1-a");
        client.create().forPath("/root/n1-b");
        client.create().forPath("/root/n1-b/n2-a");
        client.create().forPath("/root/n1-b/n2-b");
        client.create().forPath("/root/n1-b/n2-b/n3-a");
        client.create().forPath("/root/n1-c");
        client.create().forPath("/root/n1-d");

        TreeCacheSelector selector = new TreeCacheSelector() {
            @Override
            public boolean traverseChildren(String fullPath) {
                return !fullPath.equals("/root/n1-b/n2-b");
            }

            @Override
            public boolean acceptChild(String fullPath) {
                return !fullPath.equals("/root/n1-c");
            }
        };
        cache = buildWithListeners(TreeCache.newBuilder(client, "/root").setSelector(selector));
        cache.start();

        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/root");
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/root/n1-a");
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/root/n1-b");
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/root/n1-d");
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/root/n1-b/n2-a");
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/root/n1-b/n2-b");
        assertEvent(TreeCacheEvent.Type.INITIALIZED);
        assertNoMoreEvents();
    }

    @Test
    public void testStartup() throws Exception {
        client.create().forPath("/test");
        client.create().forPath("/test/1", "one".getBytes());
        client.create().forPath("/test/2", "two".getBytes());
        client.create().forPath("/test/3", "three".getBytes());
        client.create().forPath("/test/2/sub", "two-sub".getBytes());

        cache = newTreeCacheWithListeners(client, "/test");
        cache.start();
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test");
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/1", "one".getBytes());
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/2", "two".getBytes());
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/3", "three".getBytes());
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/2/sub", "two-sub".getBytes());
        assertEvent(TreeCacheEvent.Type.INITIALIZED);
        assertNoMoreEvents();

        assertEquals(cache.getCurrentChildren("/test").keySet(), ImmutableSet.of("1", "2", "3"));
        assertEquals(cache.getCurrentChildren("/test/1").keySet(), ImmutableSet.of());
        assertEquals(cache.getCurrentChildren("/test/2").keySet(), ImmutableSet.of("sub"));
        assertNull(cache.getCurrentChildren("/test/non_exist"));
    }

    @Test
    public void testCreateParents() throws Exception {
        cache = newTreeCacheWithListeners(client, "/one/two/three");
        cache.start();
        assertEvent(TreeCacheEvent.Type.INITIALIZED);
        assertNoMoreEvents();
        assertNull(client.checkExists().forPath("/one/two/three"));
        cache.close();

        cache = buildWithListeners(
                TreeCache.newBuilder(client, "/one/two/three").setCreateParentNodes(true));
        cache.start();
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/one/two/three");
        assertEvent(TreeCacheEvent.Type.INITIALIZED);
        assertNoMoreEvents();
        assertNotNull(client.checkExists().forPath("/one/two/three"));
    }

    @Test
    public void testStartEmpty() throws Exception {
        cache = newTreeCacheWithListeners(client, "/test");
        cache.start();
        assertEvent(TreeCacheEvent.Type.INITIALIZED);

        client.create().forPath("/test");
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test");
        assertNoMoreEvents();
    }

    @Test
    public void testStartEmptyDeeper() throws Exception {
        cache = newTreeCacheWithListeners(client, "/test/foo/bar");
        cache.start();
        assertEvent(TreeCacheEvent.Type.INITIALIZED);

        client.create().creatingParentsIfNeeded().forPath("/test/foo");
        assertNoMoreEvents();
        client.create().forPath("/test/foo/bar");
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/foo/bar");
        assertNoMoreEvents();
    }

    @Test
    public void testDepth0() throws Exception {
        client.create().forPath("/test");
        client.create().forPath("/test/1", "one".getBytes());
        client.create().forPath("/test/2", "two".getBytes());
        client.create().forPath("/test/3", "three".getBytes());
        client.create().forPath("/test/2/sub", "two-sub".getBytes());

        cache = buildWithListeners(TreeCache.newBuilder(client, "/test").setMaxDepth(0));
        cache.start();
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test");
        assertEvent(TreeCacheEvent.Type.INITIALIZED);
        assertNoMoreEvents();

        assertEquals(cache.getCurrentChildren("/test").keySet(), ImmutableSet.of());
        assertNull(cache.getCurrentData("/test/1"));
        assertNull(cache.getCurrentChildren("/test/1"));
        assertNull(cache.getCurrentData("/test/non_exist"));
    }

    @Test
    public void testDepth1() throws Exception {
        client.create().forPath("/test");
        client.create().forPath("/test/1", "one".getBytes());
        client.create().forPath("/test/2", "two".getBytes());
        client.create().forPath("/test/3", "three".getBytes());
        client.create().forPath("/test/2/sub", "two-sub".getBytes());

        cache = buildWithListeners(TreeCache.newBuilder(client, "/test").setMaxDepth(1));
        cache.start();
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test");
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/1", "one".getBytes());
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/2", "two".getBytes());
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/3", "three".getBytes());
        assertEvent(TreeCacheEvent.Type.INITIALIZED);
        assertNoMoreEvents();

        assertEquals(cache.getCurrentChildren("/test").keySet(), ImmutableSet.of("1", "2", "3"));
        assertEquals(cache.getCurrentChildren("/test/1").keySet(), ImmutableSet.of());
        assertEquals(cache.getCurrentChildren("/test/2").keySet(), ImmutableSet.of());
        assertNull(cache.getCurrentData("/test/2/sub"));
        assertNull(cache.getCurrentChildren("/test/2/sub"));
        assertNull(cache.getCurrentChildren("/test/non_exist"));
    }

    @Test
    public void testDepth1Deeper() throws Exception {
        client.create().forPath("/test");
        client.create().forPath("/test/foo");
        client.create().forPath("/test/foo/bar");
        client.create().forPath("/test/foo/bar/1", "one".getBytes());
        client.create().forPath("/test/foo/bar/2", "two".getBytes());
        client.create().forPath("/test/foo/bar/3", "three".getBytes());
        client.create().forPath("/test/foo/bar/2/sub", "two-sub".getBytes());

        cache = buildWithListeners(TreeCache.newBuilder(client, "/test/foo/bar").setMaxDepth(1));
        cache.start();
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/foo/bar");
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/foo/bar/1", "one".getBytes());
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/foo/bar/2", "two".getBytes());
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/foo/bar/3", "three".getBytes());
        assertEvent(TreeCacheEvent.Type.INITIALIZED);
        assertNoMoreEvents();
    }

    @Test
    public void testAsyncInitialPopulation() throws Exception {
        client.create().forPath("/test");
        client.create().forPath("/test/one", "hey there".getBytes());

        cache = newTreeCacheWithListeners(client, "/test");
        cache.start();
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test");
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/one");
        assertEvent(TreeCacheEvent.Type.INITIALIZED);
        assertNoMoreEvents();
    }

    @Test
    public void testFromRoot() throws Exception {
        client.create().forPath("/test");
        client.create().forPath("/test/one", "hey there".getBytes());

        cache = newTreeCacheWithListeners(client, "/");
        cache.start();
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/");
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test");
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/one");
        assertEvent(TreeCacheEvent.Type.INITIALIZED);
        assertNoMoreEvents();

        assertTrue(cache.getCurrentChildren("/").keySet().contains("test"));
        assertEquals(cache.getCurrentChildren("/test").keySet(), ImmutableSet.of("one"));
        assertEquals(cache.getCurrentChildren("/test/one").keySet(), ImmutableSet.of());
        assertEquals(new String(cache.getCurrentData("/test/one").getData()), "hey there");
    }

    @Test
    public void testFromRootWithDepth() throws Exception {
        client.create().forPath("/test");
        client.create().forPath("/test/one", "hey there".getBytes());

        cache = buildWithListeners(TreeCache.newBuilder(client, "/").setMaxDepth(1));
        cache.start();
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/");
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test");
        assertEvent(TreeCacheEvent.Type.INITIALIZED);
        assertNoMoreEvents();

        assertTrue(cache.getCurrentChildren("/").keySet().contains("test"));
        assertEquals(cache.getCurrentChildren("/test").keySet(), ImmutableSet.of());
        assertNull(cache.getCurrentData("/test/one"));
        assertNull(cache.getCurrentChildren("/test/one"));
    }

    @Test
    public void testWithNamespace() throws Exception {
        client.create().forPath("/outer");
        client.create().forPath("/outer/foo");
        client.create().forPath("/outer/test");
        client.create().forPath("/outer/test/one", "hey there".getBytes());

        cache = newTreeCacheWithListeners(client.usingNamespace("outer"), "/test");
        cache.start();
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test");
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/one");
        assertEvent(TreeCacheEvent.Type.INITIALIZED);
        assertNoMoreEvents();

        assertEquals(cache.getCurrentChildren("/test").keySet(), ImmutableSet.of("one"));
        assertEquals(cache.getCurrentChildren("/test/one").keySet(), ImmutableSet.of());
        assertEquals(new String(cache.getCurrentData("/test/one").getData()), "hey there");
    }

    @Test
    public void testWithNamespaceAtRoot() throws Exception {
        client.create().forPath("/outer");
        client.create().forPath("/outer/foo");
        client.create().forPath("/outer/test");
        client.create().forPath("/outer/test/one", "hey there".getBytes());

        cache = newTreeCacheWithListeners(client.usingNamespace("outer"), "/");
        cache.start();
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/");
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/foo");
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test");
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/one");
        assertEvent(TreeCacheEvent.Type.INITIALIZED);
        assertNoMoreEvents();
        assertEquals(cache.getCurrentChildren("/").keySet(), ImmutableSet.of("foo", "test"));
        assertEquals(cache.getCurrentChildren("/foo").keySet(), ImmutableSet.of());
        assertEquals(cache.getCurrentChildren("/test").keySet(), ImmutableSet.of("one"));
        assertEquals(cache.getCurrentChildren("/test/one").keySet(), ImmutableSet.of());
        assertEquals(new String(cache.getCurrentData("/test/one").getData()), "hey there");
    }

    @Test
    public void testSyncInitialPopulation() throws Exception {
        cache = newTreeCacheWithListeners(client, "/test");
        cache.start();
        assertEvent(TreeCacheEvent.Type.INITIALIZED);

        client.create().forPath("/test");
        client.create().forPath("/test/one", "hey there".getBytes());
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test");
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/one");
        assertNoMoreEvents();
    }

    @Test
    public void testChildrenInitialized() throws Exception {
        client.create().forPath("/test", "".getBytes());
        client.create().forPath("/test/1", "1".getBytes());
        client.create().forPath("/test/2", "2".getBytes());
        client.create().forPath("/test/3", "3".getBytes());

        cache = newTreeCacheWithListeners(client, "/test");
        cache.start();
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test");
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/1");
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/2");
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/3");
        assertEvent(TreeCacheEvent.Type.INITIALIZED);
        assertNoMoreEvents();
    }

    @Test
    public void testUpdateWhenNotCachingData() throws Exception {
        client.create().forPath("/test");

        cache = buildWithListeners(TreeCache.newBuilder(client, "/test").setCacheData(false));
        cache.start();
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test");
        assertEvent(TreeCacheEvent.Type.INITIALIZED);

        client.create().forPath("/test/foo", "first".getBytes());
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/foo");

        client.setData().forPath("/test/foo", "something new".getBytes());
        assertEvent(TreeCacheEvent.Type.NODE_UPDATED, "/test/foo");
        assertNoMoreEvents();

        assertNotNull(cache.getCurrentData("/test/foo"));
        // No byte data querying the tree because we're not caching data.
        assertNull(cache.getCurrentData("/test/foo").getData());
    }

    @Test
    public void testDeleteThenCreate() throws Exception {
        client.create().forPath("/test");
        client.create().forPath("/test/foo", "one".getBytes());

        cache = newTreeCacheWithListeners(client, "/test");
        cache.start();
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test");
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/foo");
        assertEvent(TreeCacheEvent.Type.INITIALIZED);

        client.delete().forPath("/test/foo");
        assertEvent(TreeCacheEvent.Type.NODE_REMOVED, "/test/foo", "one".getBytes());
        client.create().forPath("/test/foo", "two".getBytes());
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/foo");

        client.delete().forPath("/test/foo");
        assertEvent(TreeCacheEvent.Type.NODE_REMOVED, "/test/foo", "two".getBytes());
        client.create().forPath("/test/foo", "two".getBytes());
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/foo");

        assertNoMoreEvents();
    }

    @Test
    public void testDeleteThenCreateRoot() throws Exception {
        client.create().forPath("/test");
        client.create().forPath("/test/foo", "one".getBytes());

        cache = newTreeCacheWithListeners(client, "/test/foo");
        cache.start();
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/foo");
        assertEvent(TreeCacheEvent.Type.INITIALIZED);

        client.delete().forPath("/test/foo");
        assertEvent(TreeCacheEvent.Type.NODE_REMOVED, "/test/foo");
        client.create().forPath("/test/foo", "two".getBytes());
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/foo");

        client.delete().forPath("/test/foo");
        assertEvent(TreeCacheEvent.Type.NODE_REMOVED, "/test/foo");
        client.create().forPath("/test/foo", "two".getBytes());
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/foo");

        assertNoMoreEvents();
    }

    @Test
    public void testKilledSession() throws Exception {
        client.create().forPath("/test");

        cache = newTreeCacheWithListeners(client, "/test");
        cache.start();
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test");
        assertEvent(TreeCacheEvent.Type.INITIALIZED);

        client.create().forPath("/test/foo", "foo".getBytes());
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/foo");
        client.create().withMode(CreateMode.EPHEMERAL).forPath("/test/me", "data".getBytes());
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/me");

        client.getZookeeperClient().getZooKeeper().getTestable().injectSessionExpiration();
        assertEvent(TreeCacheEvent.Type.INITIALIZED, null, null, true);
        assertEvent(TreeCacheEvent.Type.NODE_REMOVED, "/test/me", "data".getBytes(), true);

        assertNoMoreEvents();
    }

    @Test
    public void testBasics() throws Exception {
        client.create().forPath("/test");

        cache = newTreeCacheWithListeners(client, "/test");
        cache.start();
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test");
        assertEvent(TreeCacheEvent.Type.INITIALIZED);
        assertEquals(cache.getCurrentChildren("/test").keySet(), ImmutableSet.of());
        assertNull(cache.getCurrentChildren("/t"));
        assertNull(cache.getCurrentChildren("/testing"));

        client.create().forPath("/test/one", "hey there".getBytes());
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/one");
        assertEquals(cache.getCurrentChildren("/test").keySet(), ImmutableSet.of("one"));
        assertEquals(new String(cache.getCurrentData("/test/one").getData()), "hey there");
        assertEquals(cache.getCurrentChildren("/test/one").keySet(), ImmutableSet.of());
        assertNull(cache.getCurrentChildren("/test/o"));
        assertNull(cache.getCurrentChildren("/test/onely"));

        client.setData().forPath("/test/one", "sup!".getBytes());
        assertEvent(TreeCacheEvent.Type.NODE_UPDATED, "/test/one");
        assertEquals(cache.getCurrentChildren("/test").keySet(), ImmutableSet.of("one"));
        assertEquals(new String(cache.getCurrentData("/test/one").getData()), "sup!");

        client.delete().forPath("/test/one");
        assertEvent(TreeCacheEvent.Type.NODE_REMOVED, "/test/one", "sup!".getBytes());
        assertEquals(cache.getCurrentChildren("/test").keySet(), ImmutableSet.of());

        assertNoMoreEvents();
    }

    @Test
    public void testBasicsWithNoZkWatches() throws Exception {
        client.create().forPath("/test");
        client.create().forPath("/test/one", "hey there".getBytes());

        cache = buildWithListeners(TreeCache.newBuilder(client, "/test").disableZkWatches(true));

        cache.start();
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test");
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/one");

        assertEvent(TreeCacheEvent.Type.INITIALIZED);
        assertEquals(cache.getCurrentChildren("/test").keySet(), ImmutableSet.of("one"));
        assertEquals(new String(cache.getCurrentData("/test/one").getData()), "hey there");
        assertEquals(cache.getCurrentChildren("/test/one").keySet(), ImmutableSet.of());
        assertNull(cache.getCurrentChildren("/test/o"));
        assertNull(cache.getCurrentChildren("/test/onely"));
        assertNull(cache.getCurrentChildren("/t"));
        assertNull(cache.getCurrentChildren("/testing"));

        assertNoMoreEvents();
    }

    @Test
    public void testBasicsOnTwoCaches() throws Exception {
        TreeCache cache2 = newTreeCacheWithListeners(client, "/test");
        cache2.getListenable().removeListener(eventListener); // Don't listen on the second cache.

        // Just ensures the same event count; enables test flow control on cache2.
        final Semaphore semaphore = new Semaphore(0);
        cache2.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
                semaphore.release();
            }
        });

        try {
            client.create().forPath("/test");

            cache = newTreeCacheWithListeners(client, "/test");
            cache.start();
            cache2.start();

            assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test");
            assertEvent(TreeCacheEvent.Type.INITIALIZED);
            semaphore.acquire(2);

            client.create().forPath("/test/one", "hey there".getBytes());
            assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/one");
            assertEquals(new String(cache.getCurrentData("/test/one").getData()), "hey there");
            semaphore.acquire();
            assertEquals(new String(cache2.getCurrentData("/test/one").getData()), "hey there");

            client.setData().forPath("/test/one", "sup!".getBytes());
            assertEvent(TreeCacheEvent.Type.NODE_UPDATED, "/test/one");
            assertEquals(new String(cache.getCurrentData("/test/one").getData()), "sup!");
            semaphore.acquire();
            assertEquals(new String(cache2.getCurrentData("/test/one").getData()), "sup!");

            client.delete().forPath("/test/one");
            assertEvent(TreeCacheEvent.Type.NODE_REMOVED, "/test/one", "sup!".getBytes());
            assertNull(cache.getCurrentData("/test/one"));
            semaphore.acquire();
            assertNull(cache2.getCurrentData("/test/one"));

            assertNoMoreEvents();
            assertEquals(semaphore.availablePermits(), 0);
        } finally {
            CloseableUtils.closeQuietly(cache2);
        }
    }

    @Test
    public void testDeleteNodeAfterCloseDoesntCallExecutor() throws Exception {
        client.create().forPath("/test");

        cache = newTreeCacheWithListeners(client, "/test");
        cache.start();
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test");
        assertEvent(TreeCacheEvent.Type.INITIALIZED);

        client.create().forPath("/test/one", "hey there".getBytes());
        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test/one");
        assertEquals(new String(cache.getCurrentData("/test/one").getData()), "hey there");

        cache.close();
        assertNoMoreEvents();

        client.delete().forPath("/test/one");
        assertNoMoreEvents();
    }

    /**
     * Make sure TreeCache gets to a sane state when we can't initially connect to server.
     */
    @Test
    public void testServerNotStartedYet() throws Exception {
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
        assertEvent(TreeCacheEvent.Type.INITIALIZED);

        client.create().forPath("/test");

        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test");
        assertNoMoreEvents();
    }

    @Test
    public void testErrorListener() throws Exception {
        client.create().forPath("/test");

        cache = buildWithListeners(TreeCache.newBuilder(client, "/test"));

        // Register a listener that throws an exception for the event
        cache.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
                if (event.getType() == Type.NODE_UPDATED) {
                    throw new RuntimeException("Test Exception");
                }
            }
        });

        cache.getUnhandledErrorListenable().removeListener(errorListener);
        final AtomicBoolean isProcessed = new AtomicBoolean(false);
        cache.getUnhandledErrorListenable().addListener(new UnhandledErrorListener() {
            @Override
            public void unhandledError(String message, Throwable e) {
                assertFalse(isProcessed.compareAndSet(false, true));
            }
        });

        cache.start();

        assertEvent(TreeCacheEvent.Type.NODE_ADDED, "/test");
        assertEvent(TreeCacheEvent.Type.INITIALIZED);

        client.setData().forPath("/test", "hey there".getBytes());
        assertEvent(TreeCacheEvent.Type.NODE_UPDATED, "/test");

        assertNoMoreEvents();
    }
}
