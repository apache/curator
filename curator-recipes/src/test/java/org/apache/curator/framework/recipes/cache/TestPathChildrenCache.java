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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import io.github.artsok.RepeatedIfExceptionsTest;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.imps.TestCleanState;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.ExecuteCalledWatchingExecutorService;
import org.apache.curator.test.TestingServer;
import org.apache.curator.test.Timing;
import org.apache.curator.test.compatibility.CuratorTestBase;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.Tag;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@Tag(CuratorTestBase.zk35TestCompatibilityGroup)
public class TestPathChildrenCache extends BaseClassForTests
{
    @RepeatedIfExceptionsTest(repeats = BaseClassForTests.REPEATS)
    public void testParentContainerMissing() throws Exception
    {
        Timing timing = new Timing();
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        PathChildrenCache cache = new PathChildrenCache(client, "/a/b/test", true);
        try
        {
            client.start();
            CountDownLatch startedLatch = new CountDownLatch(1);
            client.getConnectionStateListenable().addListener((__, newState) -> {
                if ( newState == ConnectionState.CONNECTED )
                {
                    startedLatch.countDown();
                }
            });
            assertTrue(timing.awaitLatch(startedLatch));

            final BlockingQueue<PathChildrenCacheEvent.Type> events = Queues.newLinkedBlockingQueue();
            PathChildrenCacheListener listener = new PathChildrenCacheListener()
            {
                @Override
                public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
                {
                    events.add(event.getType());
                }
            };
            cache.getListenable().addListener(listener);
            cache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
            assertEquals(events.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS), PathChildrenCacheEvent.Type.INITIALIZED);

            client.create().forPath("/a/b/test/one");
            client.create().forPath("/a/b/test/two");
            assertEquals(events.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS), PathChildrenCacheEvent.Type.CHILD_ADDED);
            assertEquals(events.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS), PathChildrenCacheEvent.Type.CHILD_ADDED);

            client.delete().forPath("/a/b/test/one");
            client.delete().forPath("/a/b/test/two");
            client.delete().forPath("/a/b/test");
            assertEquals(events.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS), PathChildrenCacheEvent.Type.CHILD_REMOVED);
            assertEquals(events.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS), PathChildrenCacheEvent.Type.CHILD_REMOVED);

            timing.sleepABit();

            client.create().creatingParentContainersIfNeeded().forPath("/a/b/test/new");
            assertEquals(events.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS), PathChildrenCacheEvent.Type.CHILD_ADDED);
        }
        finally
        {
            CloseableUtils.closeQuietly(cache);
            CloseableUtils.closeQuietly(client);
        }
    }

    @RepeatedIfExceptionsTest(repeats = BaseClassForTests.REPEATS)
    public void testInitializedEvenIfChildDeleted() throws Exception
    {
        final CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));

        PathChildrenCache cache = new PathChildrenCache(client, "/a/b/test", true)
        {
            @Override
            void getDataAndStat(final String fullPath) throws Exception
            {
                // before installing a data watcher on the child, let's delete this child
                client.delete().forPath("/a/b/test/one");
                super.getDataAndStat(fullPath);
            }
        };

        Timing timing = new Timing();

        try
        {
            client.start();

            final CountDownLatch cacheInitialized = new CountDownLatch(1);

            PathChildrenCacheListener listener = new PathChildrenCacheListener()
            {
                @Override
                public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
                {
                    if ( event.getType() == PathChildrenCacheEvent.Type.INITIALIZED )
                    {
                        cacheInitialized.countDown();
                    }
                }
            };
            cache.getListenable().addListener(listener);

            client.create().creatingParentsIfNeeded().forPath("/a/b/test/one");

            cache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);

            assertTrue(timing.awaitLatch(cacheInitialized));
            assertEquals(cache.getCurrentData().size(), 0);
        }
        finally
        {
            CloseableUtils.closeQuietly(cache);
            CloseableUtils.closeQuietly(client);
        }
    }

    @RepeatedIfExceptionsTest(repeats = BaseClassForTests.REPEATS)
    public void testWithBadConnect() throws Exception
    {
        final int serverPort = server.getPort();
        server.close();

        Timing timing = new Timing();
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), 1000, 1000, new RetryOneTime(1));
        try
        {
            client.start();

            final CountDownLatch ensurePathLatch = new CountDownLatch(1);
            PathChildrenCache cache = new PathChildrenCache(client, "/", true)
            {
                @Override
                protected void ensurePath() throws Exception
                {
                    try
                    {
                        super.ensurePath();
                    }
                    catch ( Exception e )
                    {
                        ensurePathLatch.countDown();
                        throw e;
                    }
                }
            };
            final CountDownLatch addedLatch = new CountDownLatch(1);
            PathChildrenCacheListener listener = new PathChildrenCacheListener()
            {
                @Override
                public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
                {
                    if ( event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED  &&
                         event.getData().getPath().equals("/baz"))
                    {
                        addedLatch.countDown();
                    }
                }
            };
            cache.getListenable().addListener(listener);
            cache.start();
            assertTrue(timing.awaitLatch(ensurePathLatch));

            final CountDownLatch connectedLatch = new CountDownLatch(1);
            client.getConnectionStateListenable().addListener(new ConnectionStateListener()
            {

                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState)
                {
                    if(newState == ConnectionState.CONNECTED)
                    {
                        connectedLatch.countDown();
                    }
                }
            });

            server = new TestingServer(serverPort, true);

            assertTrue(timing.awaitLatch(connectedLatch));

            client.create().creatingParentContainersIfNeeded().forPath("/baz", new byte[]{1, 2, 3});

            assertNotNull(client.checkExists().forPath("/baz"), "/baz does not exist");

            assertTrue(timing.awaitLatch(addedLatch));

            assertNotNull(cache.getCurrentData("/baz"), "cache doesn't see /baz");
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @RepeatedIfExceptionsTest(repeats = BaseClassForTests.REPEATS)
    public void testPostInitializedForEmpty() throws Exception
    {
        Timing timing = new Timing();
        PathChildrenCache cache = null;
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        try
        {
            client.start();

            final CountDownLatch latch = new CountDownLatch(1);
            cache = new PathChildrenCache(client, "/test", true);
            cache.getListenable().addListener
                (
                    new PathChildrenCacheListener()
                    {
                        @Override
                        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
                        {
                            if ( event.getType() == PathChildrenCacheEvent.Type.INITIALIZED )
                            {
                                latch.countDown();
                            }
                        }
                    }
                );
            cache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
            assertTrue(timing.awaitLatch(latch));
        }
        finally
        {
            CloseableUtils.closeQuietly(cache);
            TestCleanState.closeAndTestClean(client);
        }
    }

    @RepeatedIfExceptionsTest(repeats = BaseClassForTests.REPEATS)
    public void testAsyncInitialPopulation() throws Exception
    {
        Timing timing = new Timing();
        PathChildrenCache cache = null;
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        try
        {
            client.start();

            client.create().forPath("/test");
            client.create().forPath("/test/one", "hey there".getBytes());

            final BlockingQueue<PathChildrenCacheEvent> events = new LinkedBlockingQueue<PathChildrenCacheEvent>();
            cache = new PathChildrenCache(client, "/test", true);
            cache.getListenable().addListener
                (
                    new PathChildrenCacheListener()
                    {
                        @Override
                        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
                        {
                            events.offer(event);
                        }
                    }
                );
            cache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);

            PathChildrenCacheEvent event = events.poll(timing.forWaiting().seconds(), TimeUnit.SECONDS);
            assertEquals(event.getType(), PathChildrenCacheEvent.Type.CHILD_ADDED);

            event = events.poll(timing.forWaiting().seconds(), TimeUnit.SECONDS);
            assertEquals(event.getType(), PathChildrenCacheEvent.Type.INITIALIZED);
            assertEquals(event.getInitialData().size(), 1);
        }
        finally
        {
            CloseableUtils.closeQuietly(cache);
            TestCleanState.closeAndTestClean(client);
        }
    }

    @RepeatedIfExceptionsTest(repeats = BaseClassForTests.REPEATS)
    public void testChildrenInitialized() throws Exception
    {
        Timing timing = new Timing();
        PathChildrenCache cache = null;
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        try
        {
            client.start();
            client.create().forPath("/test");

            cache = new PathChildrenCache(client, "/test", true);

            final CountDownLatch addedLatch = new CountDownLatch(3);
            final CountDownLatch initLatch = new CountDownLatch(1);
            cache.getListenable().addListener
                (
                    new PathChildrenCacheListener()
                    {
                        @Override
                        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
                        {
                            if ( event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED )
                            {
                                addedLatch.countDown();
                            }
                            else if ( event.getType() == PathChildrenCacheEvent.Type.INITIALIZED )
                            {
                                initLatch.countDown();
                            }
                        }
                    }
                );

            client.create().forPath("/test/1", "1".getBytes());
            client.create().forPath("/test/2", "2".getBytes());
            client.create().forPath("/test/3", "3".getBytes());

            cache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);

            assertTrue(timing.awaitLatch(addedLatch));
            assertTrue(timing.awaitLatch(initLatch));
            assertEquals(cache.getCurrentData().size(), 3);
            assertArrayEquals(cache.getCurrentData().get(0).getData(), "1".getBytes());
            assertArrayEquals(cache.getCurrentData().get(1).getData(), "2".getBytes());
            assertArrayEquals(cache.getCurrentData().get(2).getData(), "3".getBytes());
        }
        finally
        {
            CloseableUtils.closeQuietly(cache);
            TestCleanState.closeAndTestClean(client);
        }
    }

    @RepeatedIfExceptionsTest(repeats = BaseClassForTests.REPEATS)
    public void testChildrenInitializedNormal() throws Exception
    {
        Timing timing = new Timing();
        PathChildrenCache cache = null;
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        try
        {
            client.start();
            client.create().forPath("/test");

            cache = new PathChildrenCache(client, "/test", true);

            final CountDownLatch addedLatch = new CountDownLatch(3);
            cache.getListenable().addListener
                (
                    new PathChildrenCacheListener()
                    {
                        @Override
                        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
                        {
                            assertNotEquals(event.getType(), PathChildrenCacheEvent.Type.INITIALIZED);
                            if ( event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED )
                            {
                                addedLatch.countDown();
                            }
                        }
                    }
                );

            client.create().forPath("/test/1", "1".getBytes());
            client.create().forPath("/test/2", "2".getBytes());
            client.create().forPath("/test/3", "3".getBytes());

            cache.start(PathChildrenCache.StartMode.NORMAL);

            assertTrue(timing.awaitLatch(addedLatch));
            assertEquals(cache.getCurrentData().size(), 3);
            assertArrayEquals(cache.getCurrentData().get(0).getData(), "1".getBytes());
            assertArrayEquals(cache.getCurrentData().get(1).getData(), "2".getBytes());
            assertArrayEquals(cache.getCurrentData().get(2).getData(), "3".getBytes());
        }
        finally
        {
            CloseableUtils.closeQuietly(cache);
            TestCleanState.closeAndTestClean(client);
        }
    }

    @RepeatedIfExceptionsTest(repeats = BaseClassForTests.REPEATS)
    public void testUpdateWhenNotCachingData() throws Exception
    {
        Timing timing = new Timing();

        PathChildrenCache cache = null;
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        client.start();
        try
        {
            final CountDownLatch updatedLatch = new CountDownLatch(1);
            final CountDownLatch addedLatch = new CountDownLatch(1);
            client.create().creatingParentsIfNeeded().forPath("/test");
            cache = new PathChildrenCache(client, "/test", false);
            cache.getListenable().addListener
                (
                    new PathChildrenCacheListener()
                    {
                        @Override
                        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
                        {
                            if ( event.getType() == PathChildrenCacheEvent.Type.CHILD_UPDATED )
                            {
                                updatedLatch.countDown();
                            }
                            else if ( event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED )
                            {
                                addedLatch.countDown();
                            }
                        }
                    }
                );
            cache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);

            client.create().forPath("/test/foo", "first".getBytes());
            assertTrue(timing.awaitLatch(addedLatch));

            client.setData().forPath("/test/foo", "something new".getBytes());
            assertTrue(timing.awaitLatch(updatedLatch));
        }
        finally
        {
            CloseableUtils.closeQuietly(cache);
            TestCleanState.closeAndTestClean(client);
        }
    }

    @RepeatedIfExceptionsTest(repeats = BaseClassForTests.REPEATS)
    public void testEnsurePath() throws Exception
    {
        Timing timing = new Timing();

        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        client.start();
        try
        {
            try ( PathChildrenCache cache = new PathChildrenCache(client, "/one/two/three", false) )
            {
                cache.start();
                timing.sleepABit();

                try
                {
                    client.create().forPath("/one/two/three/four");
                }
                catch ( KeeperException.NoNodeException e )
                {
                    fail("Path should exist", e);
                }
            }
            timing.sleepABit();
        }
        finally
        {
            TestCleanState.closeAndTestClean(client);
        }
    }

    @RepeatedIfExceptionsTest(repeats = BaseClassForTests.REPEATS)
    public void testDeleteThenCreate() throws Exception
    {
        Timing timing = new Timing();
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        client.start();
        try
        {
            client.create().forPath("/test");
            client.create().forPath("/test/foo", "one".getBytes());

            final AtomicReference<Throwable> error = new AtomicReference<Throwable>();
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

            final CountDownLatch removedLatch = new CountDownLatch(1);
            final CountDownLatch postRemovedLatch = new CountDownLatch(1);
            final CountDownLatch dataLatch = new CountDownLatch(1);
            try ( PathChildrenCache cache = new PathChildrenCache(client, "/test", true) )
            {
                cache.getListenable().addListener
                    (
                        new PathChildrenCacheListener()
                        {
                            @Override
                            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
                            {
                                if ( event.getType() == PathChildrenCacheEvent.Type.CHILD_REMOVED )
                                {
                                    removedLatch.countDown();
                                    assertTrue(postRemovedLatch.await(10, TimeUnit.SECONDS));
                                }
                                else
                                {
                                    try
                                    {
                                        assertArrayEquals(event.getData().getData(), "two".getBytes());
                                    }
                                    finally
                                    {
                                        dataLatch.countDown();
                                    }
                                }
                            }
                        }
                    );
                cache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);

                client.delete().forPath("/test/foo");
                assertTrue(timing.awaitLatch(removedLatch));
                client.create().forPath("/test/foo", "two".getBytes());
                postRemovedLatch.countDown();
                assertTrue(timing.awaitLatch(dataLatch));

                Throwable t = error.get();
                if ( t != null )
                {
                    fail("Assert", t);
                }
            }
        }
        finally
        {
            TestCleanState.closeAndTestClean(client);
        }
    }

    @RepeatedIfExceptionsTest(repeats = BaseClassForTests.REPEATS)
    public void testRebuildAgainstOtherProcesses() throws Exception
    {
        Timing timing = new Timing();
        final CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        client.start();
        try
        {
            client.create().forPath("/test");
            client.create().forPath("/test/foo");
            client.create().forPath("/test/bar");
            client.create().forPath("/test/snafu", "original".getBytes());

            final CountDownLatch addedLatch = new CountDownLatch(2);
            try ( final PathChildrenCache cache = new PathChildrenCache(client, "/test", true) )
            {
                cache.getListenable().addListener
                    (
                        new PathChildrenCacheListener()
                        {
                            @Override
                            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
                            {
                                if ( event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED )
                                {
                                    if ( event.getData().getPath().equals("/test/test") )
                                    {
                                        addedLatch.countDown();
                                    }
                                }
                                else if ( event.getType() == PathChildrenCacheEvent.Type.CHILD_UPDATED )
                                {
                                    if ( event.getData().getPath().equals("/test/snafu") )
                                    {
                                        addedLatch.countDown();
                                    }
                                }
                            }
                        }
                    );
                cache.rebuildTestExchanger = new Exchanger<Object>();
                ExecutorService service = Executors.newSingleThreadExecutor();
                final AtomicReference<String> deletedPath = new AtomicReference<String>();
                Future<Object> future = service.submit
                    (
                        new Callable<Object>()
                        {
                            @Override
                            public Object call() throws Exception
                            {
                                cache.rebuildTestExchanger.exchange(new Object());

                                // simulate another process adding a node while we're rebuilding
                                client.create().forPath("/test/test");

                                List<ChildData> currentData = cache.getCurrentData();
                                assertTrue(currentData.size() > 0);

                                // simulate another process removing a node while we're rebuilding
                                client.delete().forPath(currentData.get(0).getPath());
                                deletedPath.set(currentData.get(0).getPath());

                                cache.rebuildTestExchanger.exchange(new Object());

                                ChildData childData = null;
                                while ( childData == null )
                                {
                                    childData = cache.getCurrentData("/test/snafu");
                                    Thread.sleep(1000);
                                }
                                assertArrayEquals(childData.getData(), "original".getBytes());
                                client.setData().forPath("/test/snafu", "grilled".getBytes());

                                cache.rebuildTestExchanger.exchange(new Object());

                                return null;
                            }
                        }
                    );
                cache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
                future.get();

                assertTrue(timing.awaitLatch(addedLatch));
                assertNotNull(cache.getCurrentData("/test/test"));
                assertNull(cache.getCurrentData(deletedPath.get()));
                assertArrayEquals(cache.getCurrentData("/test/snafu").getData(), "grilled".getBytes());
            }
        }
        finally
        {
            TestCleanState.closeAndTestClean(client);
        }
    }

    // see https://github.com/Netflix/curator/issues/27 - was caused by not comparing old->new data
    @RepeatedIfExceptionsTest(repeats = BaseClassForTests.REPEATS)
    public void testIssue27() throws Exception
    {
        Timing timing = new Timing();
        PathChildrenCache cache = null;
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        client.start();
        try
        {
            client.create().forPath("/base");
            client.create().forPath("/base/a");
            client.create().forPath("/base/b");
            client.create().forPath("/base/c");

            client.getChildren().forPath("/base");

            final List<PathChildrenCacheEvent.Type> events = Lists.newArrayList();
            final Semaphore semaphore = new Semaphore(0);
            cache = new PathChildrenCache(client, "/base", true);
            cache.getListenable().addListener
                (
                    new PathChildrenCacheListener()
                    {
                        @Override
                        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
                        {
                            events.add(event.getType());
                            semaphore.release();
                        }
                    }
                );
            cache.start();

            assertTrue(timing.acquireSemaphore(semaphore, 3));

            client.delete().forPath("/base/a");
            assertTrue(timing.acquireSemaphore(semaphore, 1));

            client.create().forPath("/base/a");
            assertTrue(timing.acquireSemaphore(semaphore, 1));

            List<PathChildrenCacheEvent.Type> expected = Lists.newArrayList
                (
                    PathChildrenCacheEvent.Type.CHILD_ADDED,
                    PathChildrenCacheEvent.Type.CHILD_ADDED,
                    PathChildrenCacheEvent.Type.CHILD_ADDED,
                    PathChildrenCacheEvent.Type.CHILD_REMOVED,
                    PathChildrenCacheEvent.Type.CHILD_ADDED
                );
            assertEquals(expected, events);
        }
        finally
        {
            CloseableUtils.closeQuietly(cache);
            TestCleanState.closeAndTestClean(client);
        }
    }

    // test Issue 27 using new rebuild() method
    @RepeatedIfExceptionsTest(repeats = BaseClassForTests.REPEATS)
    public void testIssue27Alt() throws Exception
    {
        Timing timing = new Timing();
        PathChildrenCache cache = null;
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        client.start();
        try
        {
            client.create().forPath("/base");
            client.create().forPath("/base/a");
            client.create().forPath("/base/b");
            client.create().forPath("/base/c");

            client.getChildren().forPath("/base");

            final List<PathChildrenCacheEvent.Type> events = Lists.newArrayList();
            final Semaphore semaphore = new Semaphore(0);
            cache = new PathChildrenCache(client, "/base", true);
            cache.getListenable().addListener
                (
                    new PathChildrenCacheListener()
                    {
                        @Override
                        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
                        {
                            events.add(event.getType());
                            semaphore.release();
                        }
                    }
                );
            cache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);

            client.delete().forPath("/base/a");
            assertTrue(timing.acquireSemaphore(semaphore, 1));

            client.create().forPath("/base/a");
            assertTrue(timing.acquireSemaphore(semaphore, 1));

            List<PathChildrenCacheEvent.Type> expected = Lists.newArrayList
                (
                    PathChildrenCacheEvent.Type.CHILD_REMOVED,
                    PathChildrenCacheEvent.Type.CHILD_ADDED
                );
            assertEquals(expected, events);
        }
        finally
        {
            CloseableUtils.closeQuietly(cache);
            TestCleanState.closeAndTestClean(client);
        }
    }

    @RepeatedIfExceptionsTest(repeats = BaseClassForTests.REPEATS)
    public void testKilledSession() throws Exception
    {
        Timing timing = new Timing();
        PathChildrenCache cache = null;
        CuratorFramework client = null;
        try
        {
            client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
            client.start();
            client.create().forPath("/test");

            cache = new PathChildrenCache(client, "/test", true);
            cache.start();

            final CountDownLatch childAddedLatch = new CountDownLatch(1);
            final CountDownLatch lostLatch = new CountDownLatch(1);
            final CountDownLatch reconnectedLatch = new CountDownLatch(1);
            final CountDownLatch removedLatch = new CountDownLatch(1);
            cache.getListenable().addListener
                (
                    new PathChildrenCacheListener()
                    {
                        @Override
                        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
                        {
                            if ( event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED )
                            {
                                childAddedLatch.countDown();
                            }
                            else if ( event.getType() == PathChildrenCacheEvent.Type.CONNECTION_LOST )
                            {
                                lostLatch.countDown();
                            }
                            else if ( event.getType() == PathChildrenCacheEvent.Type.CONNECTION_RECONNECTED )
                            {
                                reconnectedLatch.countDown();
                            }
                            else if ( event.getType() == PathChildrenCacheEvent.Type.CHILD_REMOVED )
                            {
                                removedLatch.countDown();
                            }
                        }
                    }
                );

            client.create().withMode(CreateMode.EPHEMERAL).forPath("/test/me", "data".getBytes());
            assertTrue(timing.awaitLatch(childAddedLatch));

            client.getZookeeperClient().getZooKeeper().getTestable().injectSessionExpiration();
            assertTrue(timing.awaitLatch(lostLatch));
            assertTrue(timing.awaitLatch(reconnectedLatch));
            assertTrue(timing.awaitLatch(removedLatch));
        }
        finally
        {
            CloseableUtils.closeQuietly(cache);
            TestCleanState.closeAndTestClean(client);
        }
    }

    @RepeatedIfExceptionsTest(repeats = BaseClassForTests.REPEATS)
    public void testModes() throws Exception
    {
        Timing timing = new Timing();
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        client.start();
        try
        {
            client.create().forPath("/test");

            for ( boolean cacheData : new boolean[]{false, true} )
            {
                internalTestMode(client, cacheData);

                client.delete().forPath("/test/one");
                client.delete().forPath("/test/two");
            }
        }
        finally
        {
            TestCleanState.closeAndTestClean(client);
        }
    }

    @RepeatedIfExceptionsTest(repeats = BaseClassForTests.REPEATS)
    public void testRebuildNode() throws Exception
    {
        Timing timing = new Timing();
        PathChildrenCache cache = null;
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        try
        {
            client.start();
            client.create().creatingParentsIfNeeded().forPath("/test/one", "one".getBytes());

            final CountDownLatch latch = new CountDownLatch(1);
            final AtomicInteger counter = new AtomicInteger();
            final Semaphore semaphore = new Semaphore(1);
            cache = new PathChildrenCache(client, "/test", true)
            {
                @Override
                void getDataAndStat(String fullPath) throws Exception
                {
                    semaphore.acquire();
                    counter.incrementAndGet();
                    super.getDataAndStat(fullPath);
                    latch.countDown();
                }
            };
            cache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);

            assertTrue(timing.awaitLatch(latch));

            int saveCounter = counter.get();
            client.setData().forPath("/test/one", "alt".getBytes());
            cache.rebuildNode("/test/one");
            assertArrayEquals(cache.getCurrentData("/test/one").getData(), "alt".getBytes());
            assertEquals(saveCounter, counter.get());

            semaphore.release(1000);
            timing.sleepABit();
        }
        finally
        {
            CloseableUtils.closeQuietly(cache);
            TestCleanState.closeAndTestClean(client);
        }
    }

    private void internalTestMode(CuratorFramework client, boolean cacheData) throws Exception
    {
        try ( PathChildrenCache cache = new PathChildrenCache(client, "/test", cacheData) )
        {
            final CountDownLatch latch = new CountDownLatch(2);
            cache.getListenable().addListener
                (
                    new PathChildrenCacheListener()
                    {
                        @Override
                        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
                        {
                            if ( event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED )
                            {
                                latch.countDown();
                            }
                        }
                    }
                );
            cache.start();

            client.create().forPath("/test/one", "one".getBytes());
            client.create().forPath("/test/two", "two".getBytes());
            assertTrue(latch.await(10, TimeUnit.SECONDS));

            for ( ChildData data : cache.getCurrentData() )
            {
                if ( cacheData )
                {
                    assertNotNull(data.getData());
                    assertNotNull(data.getStat());
                }
                else
                {
                    assertNull(data.getData());
                    assertNotNull(data.getStat());
                }
            }
        }
    }

    @RepeatedIfExceptionsTest(repeats = BaseClassForTests.REPEATS)
    public void testBasics() throws Exception
    {
        Timing timing = new Timing();
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        client.start();
        try
        {
            client.create().forPath("/test");

            final BlockingQueue<PathChildrenCacheEvent.Type> events = new LinkedBlockingQueue<PathChildrenCacheEvent.Type>();
            try ( PathChildrenCache cache = new PathChildrenCache(client, "/test", true) )
            {
                cache.getListenable().addListener
                    (
                        new PathChildrenCacheListener()
                        {
                            @Override
                            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
                            {
                                if ( event.getData().getPath().equals("/test/one") )
                                {
                                    events.offer(event.getType());
                                }
                            }
                        }
                    );
                cache.start();

                client.create().forPath("/test/one", "hey there".getBytes());
                assertEquals(events.poll(timing.forWaiting().seconds(), TimeUnit.SECONDS), PathChildrenCacheEvent.Type.CHILD_ADDED);

                client.setData().forPath("/test/one", "sup!".getBytes());
                assertEquals(events.poll(timing.forWaiting().seconds(), TimeUnit.SECONDS), PathChildrenCacheEvent.Type.CHILD_UPDATED);
                assertEquals(new String(cache.getCurrentData("/test/one").getData()), "sup!");

                client.delete().forPath("/test/one");
                assertEquals(events.poll(timing.forWaiting().seconds(), TimeUnit.SECONDS), PathChildrenCacheEvent.Type.CHILD_REMOVED);
            }
        }
        finally
        {
            TestCleanState.closeAndTestClean(client);
        }
    }

    @RepeatedIfExceptionsTest(repeats = BaseClassForTests.REPEATS)
    public void testBasicsOnTwoCachesWithSameExecutor() throws Exception
    {
        Timing timing = new Timing();
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        client.start();
        try
        {
            client.create().forPath("/test");

            final BlockingQueue<PathChildrenCacheEvent.Type> events = new LinkedBlockingQueue<PathChildrenCacheEvent.Type>();
            final ExecutorService exec = Executors.newSingleThreadExecutor();
            try ( PathChildrenCache cache = new PathChildrenCache(client, "/test", true, false, exec) )
            {
                cache.getListenable().addListener
                    (
                        new PathChildrenCacheListener()
                        {
                            @Override
                            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
                            {
                                if ( event.getData().getPath().equals("/test/one") )
                                {
                                    events.offer(event.getType());
                                }
                            }
                        }
                    );
                cache.start();

                final BlockingQueue<PathChildrenCacheEvent.Type> events2 = new LinkedBlockingQueue<PathChildrenCacheEvent.Type>();
                try ( PathChildrenCache cache2 = new PathChildrenCache(client, "/test", true, false, exec) )
                {
                    cache2.getListenable().addListener(
                        new PathChildrenCacheListener()
                        {
                            @Override
                            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)
                                throws Exception
                            {
                                if ( event.getData().getPath().equals("/test/one") )
                                {
                                    events2.offer(event.getType());
                                }
                            }
                        }
                                                      );
                    cache2.start();

                    client.create().forPath("/test/one", "hey there".getBytes());
                    assertEquals(events.poll(timing.forWaiting().seconds(), TimeUnit.SECONDS), PathChildrenCacheEvent.Type.CHILD_ADDED);
                    assertEquals(events2.poll(timing.forWaiting().seconds(), TimeUnit.SECONDS), PathChildrenCacheEvent.Type.CHILD_ADDED);

                    client.setData().forPath("/test/one", "sup!".getBytes());
                    assertEquals(events.poll(timing.forWaiting().seconds(), TimeUnit.SECONDS), PathChildrenCacheEvent.Type.CHILD_UPDATED);
                    assertEquals(events2.poll(timing.forWaiting().seconds(), TimeUnit.SECONDS), PathChildrenCacheEvent.Type.CHILD_UPDATED);
                    assertEquals(new String(cache.getCurrentData("/test/one").getData()), "sup!");
                    assertEquals(new String(cache2.getCurrentData("/test/one").getData()), "sup!");

                    client.delete().forPath("/test/one");
                    assertEquals(events.poll(timing.forWaiting().seconds(), TimeUnit.SECONDS), PathChildrenCacheEvent.Type.CHILD_REMOVED);
                    assertEquals(events2.poll(timing.forWaiting().seconds(), TimeUnit.SECONDS), PathChildrenCacheEvent.Type.CHILD_REMOVED);
                }
            }
        }
        finally
        {
            TestCleanState.closeAndTestClean(client);
        }
    }

    @RepeatedIfExceptionsTest(repeats = BaseClassForTests.REPEATS)
    public void testDeleteNodeAfterCloseDoesntCallExecutor()
        throws Exception
    {
        Timing timing = new Timing();
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        client.start();
        try
        {
            client.create().forPath("/test");

            final ExecuteCalledWatchingExecutorService exec = new ExecuteCalledWatchingExecutorService(Executors.newSingleThreadExecutor());
            try ( PathChildrenCache cache = new PathChildrenCache(client, "/test", true, false, exec) )
            {
                cache.start();
                client.create().forPath("/test/one", "hey there".getBytes());

                cache.rebuild();
                assertEquals(new String(cache.getCurrentData("/test/one").getData()), "hey there");
                assertTrue(exec.isExecuteCalled());

                exec.setExecuteCalled(false);
            }
            assertFalse(exec.isExecuteCalled());

            client.delete().forPath("/test/one");
            timing.sleepABit();
            assertFalse(exec.isExecuteCalled());
        }
        finally
        {
            TestCleanState.closeAndTestClean(client);
        }

    }

    /**
     * Tests the case where there's an outstanding operation being executed when the cache is
     * shut down. See CURATOR-121, this was causing misleading warning messages to be logged.
     * @throws Exception
     */
    @RepeatedIfExceptionsTest(repeats = BaseClassForTests.REPEATS)
    public void testInterruptedOperationOnShutdown() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), 30000, 30000, new RetryOneTime(1));
        client.start();

        try
        {
            final CountDownLatch latch = new CountDownLatch(1);
            try ( final PathChildrenCache cache = new PathChildrenCache(client, "/test", false) {
                @Override
                protected void handleException(Throwable e)
                {
                    latch.countDown();
                }
            } )
            {
                cache.start();

                cache.offerOperation(new Operation()
                {

                    @Override
                    public void invoke() throws Exception
                    {
                        Thread.sleep(5000);
                    }
                });

                Thread.sleep(1000);

            }

            latch.await(5, TimeUnit.SECONDS);

            assertTrue(latch.getCount() == 1, "Unexpected exception occurred");
        }
        finally
        {
            TestCleanState.closeAndTestClean(client);
        }
    }
}
