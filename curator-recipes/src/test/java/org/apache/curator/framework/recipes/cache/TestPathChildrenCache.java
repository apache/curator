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

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.Pathable;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.imps.TestCleanState;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.KillSession;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.apache.log4j.Appender;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Exchanger;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class TestPathChildrenCache extends BaseClassForTests
{
    @Test
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
            Assert.assertTrue(timing.awaitLatch(latch));
        }
        finally
        {
            CloseableUtils.closeQuietly(cache);
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
    public void testClientClosedDuringRefreshErrorMessage() throws Exception
    {
        // Fiddle with logging so we can intercept the error events for org.apache.curator
        final List<LoggingEvent> events = Lists.newArrayList();
        Collection<String> messages = Collections2.transform(events, new Function<LoggingEvent, String>() {
            @Override
            public String apply(LoggingEvent loggingEvent) {
                return loggingEvent.getRenderedMessage();
            }
        });
        Appender appender = new AppenderSkeleton(true) {
            @Override
            protected void append(LoggingEvent event) {
                if (event.getLevel().equals(Level.ERROR)) {
                    events.add(event);
                }
            }

            @Override
            public void close() {

            }

            @Override
            public boolean requiresLayout() {
                return false;
            }
        };
        appender.setLayout(new SimpleLayout());
        Logger logger = Logger.getLogger("org.apache.curator");
        logger.addAppender(appender);

        // Check that we can intercept error log messages from the client
        CuratorFramework clientTestLogSetup = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        clientTestLogSetup.start();
        try {
            Pathable<byte[]> callback = clientTestLogSetup.getData().inBackground(new BackgroundCallback() {
                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                    // ignore result
                }
            });
            CloseableUtils.closeQuietly(clientTestLogSetup);
            callback.forPath("/test/aaa"); // this should cause an error log message
        } catch (IllegalStateException ise) {
            // ok, excpected
        } finally {
            CloseableUtils.closeQuietly(clientTestLogSetup);
        }

        Assert.assertTrue(messages.contains("Background exception was not retry-able or retry gave up"),
                "The expected error was not logged. This is an indication that this test could be broken due to" +
                        " an incomplete logging setup.");

        // try to reproduce a bunch of times because it doesn't happen reliably
        for (int i = 0; i < 50; i++) {
            CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
            client.start();
            try {
                PathChildrenCache cache = new PathChildrenCache(client, "/test", true);
                cache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
                client.newNamespaceAwareEnsurePath("/test/aaa").ensure(client.getZookeeperClient());
                client.setData().forPath("/test/aaa", new byte[]{1, 2, 3, 4, 5});
                cache.rebuildNode("/test/aaa");
                CloseableUtils.closeQuietly(cache);
            } finally {
                CloseableUtils.closeQuietly(client);
            }
        }

        Assert.assertEquals(messages.size(), 1, "There should not be any error events except for the test message, " +
                "but got:\n" + Joiner.on("\n").join(messages));

    }

    @Test
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
            Assert.assertEquals(event.getType(), PathChildrenCacheEvent.Type.CHILD_ADDED);

            event = events.poll(timing.forWaiting().seconds(), TimeUnit.SECONDS);
            Assert.assertEquals(event.getType(), PathChildrenCacheEvent.Type.INITIALIZED);
            Assert.assertEquals(event.getInitialData().size(), 1);
        }
        finally
        {
            CloseableUtils.closeQuietly(cache);
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
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

            Assert.assertTrue(timing.awaitLatch(addedLatch));
            Assert.assertTrue(timing.awaitLatch(initLatch));
            Assert.assertEquals(cache.getCurrentData().size(), 3);
            Assert.assertEquals(cache.getCurrentData().get(0).getData(), "1".getBytes());
            Assert.assertEquals(cache.getCurrentData().get(1).getData(), "2".getBytes());
            Assert.assertEquals(cache.getCurrentData().get(2).getData(), "3".getBytes());
        }
        finally
        {
            CloseableUtils.closeQuietly(cache);
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
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
                                    Assert.assertNotEquals(event.getType(), PathChildrenCacheEvent.Type.INITIALIZED);
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

            Assert.assertTrue(timing.awaitLatch(addedLatch));
            Assert.assertEquals(cache.getCurrentData().size(), 3);
            Assert.assertEquals(cache.getCurrentData().get(0).getData(), "1".getBytes());
            Assert.assertEquals(cache.getCurrentData().get(1).getData(), "2".getBytes());
            Assert.assertEquals(cache.getCurrentData().get(2).getData(), "3".getBytes());
        }
        finally
        {
            CloseableUtils.closeQuietly(cache);
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
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
            Assert.assertTrue(timing.awaitLatch(addedLatch));

            client.setData().forPath("/test/foo", "something new".getBytes());
            Assert.assertTrue(timing.awaitLatch(updatedLatch));
        }
        finally
        {
            CloseableUtils.closeQuietly(cache);
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
    public void testEnsurePath() throws Exception
    {
        Timing timing = new Timing();

        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        client.start();
        try
        {
            PathChildrenCache cache = new PathChildrenCache(client, "/one/two/three", false);
            cache.start();
            timing.sleepABit();

            try
            {
                client.create().forPath("/one/two/three/four");
            }
            catch ( KeeperException.NoNodeException e )
            {
                Assert.fail("Path should exist", e);
            }
        }
        finally
        {
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
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
            PathChildrenCache cache = new PathChildrenCache(client, "/test", true);
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
                                Assert.assertTrue(postRemovedLatch.await(10, TimeUnit.SECONDS));
                            }
                            else
                            {
                                try
                                {
                                    Assert.assertEquals(event.getData().getData(), "two".getBytes());
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
            Assert.assertTrue(timing.awaitLatch(removedLatch));
            client.create().forPath("/test/foo", "two".getBytes());
            postRemovedLatch.countDown();
            Assert.assertTrue(timing.awaitLatch(dataLatch));

            Throwable t = error.get();
            if ( t != null )
            {
                Assert.fail("Assert", t);
            }

            cache.close();
        }
        finally
        {
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
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
            final PathChildrenCache cache = new PathChildrenCache(client, "/test", true);
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
                            Assert.assertTrue(currentData.size() > 0);

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
                            Assert.assertEquals(childData.getData(), "original".getBytes());
                            client.setData().forPath("/test/snafu", "grilled".getBytes());

                            cache.rebuildTestExchanger.exchange(new Object());

                            return null;
                        }
                    }
                );
            cache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
            future.get();

            Assert.assertTrue(timing.awaitLatch(addedLatch));
            Assert.assertNotNull(cache.getCurrentData("/test/test"));
            Assert.assertNull(cache.getCurrentData(deletedPath.get()));
            Assert.assertEquals(cache.getCurrentData("/test/snafu").getData(), "grilled".getBytes());

            cache.close();
        }
        finally
        {
            TestCleanState.closeAndTestClean(client);
        }
    }

    // see https://github.com/Netflix/curator/issues/27 - was caused by not comparing old->new data
    @Test
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

            Assert.assertTrue(timing.acquireSemaphore(semaphore, 3));

            client.delete().forPath("/base/a");
            Assert.assertTrue(timing.acquireSemaphore(semaphore, 1));

            client.create().forPath("/base/a");
            Assert.assertTrue(timing.acquireSemaphore(semaphore, 1));

            List<PathChildrenCacheEvent.Type> expected = Lists.newArrayList
                (
                    PathChildrenCacheEvent.Type.CHILD_ADDED,
                    PathChildrenCacheEvent.Type.CHILD_ADDED,
                    PathChildrenCacheEvent.Type.CHILD_ADDED,
                    PathChildrenCacheEvent.Type.CHILD_REMOVED,
                    PathChildrenCacheEvent.Type.CHILD_ADDED
                );
            Assert.assertEquals(expected, events);
        }
        finally
        {
            CloseableUtils.closeQuietly(cache);
            TestCleanState.closeAndTestClean(client);
        }
    }

    // test Issue 27 using new rebuild() method
    @Test
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
            Assert.assertTrue(timing.acquireSemaphore(semaphore, 1));

            client.create().forPath("/base/a");
            Assert.assertTrue(timing.acquireSemaphore(semaphore, 1));

            List<PathChildrenCacheEvent.Type> expected = Lists.newArrayList
                (
                    PathChildrenCacheEvent.Type.CHILD_REMOVED,
                    PathChildrenCacheEvent.Type.CHILD_ADDED
                );
            Assert.assertEquals(expected, events);
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
            Assert.assertTrue(timing.awaitLatch(childAddedLatch));

            KillSession.kill(client.getZookeeperClient().getZooKeeper(), server.getConnectString());
            Assert.assertTrue(timing.awaitLatch(lostLatch));
            Assert.assertTrue(timing.awaitLatch(reconnectedLatch));
            Assert.assertTrue(timing.awaitLatch(removedLatch));
        }
        finally
        {
            CloseableUtils.closeQuietly(cache);
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
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

    @Test
    public void testRebuildNode() throws Exception
    {
        Timing timing = new Timing();
        PathChildrenCache cache = null;
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        client.start();
        try
        {
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

            latch.await();

            int saveCounter = counter.get();
            client.setData().forPath("/test/one", "alt".getBytes());
            cache.rebuildNode("/test/one");
            Assert.assertEquals(cache.getCurrentData("/test/one").getData(), "alt".getBytes());
            Assert.assertEquals(saveCounter, counter.get());

            semaphore.release(1000);
        }
        finally
        {
            CloseableUtils.closeQuietly(cache);
            TestCleanState.closeAndTestClean(client);
        }
    }

    private void internalTestMode(CuratorFramework client, boolean cacheData) throws Exception
    {
        PathChildrenCache cache = new PathChildrenCache(client, "/test", cacheData);

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
        Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));

        for ( ChildData data : cache.getCurrentData() )
        {
            if ( cacheData )
            {
                Assert.assertNotNull(data.getData());
                Assert.assertNotNull(data.getStat());
            }
            else
            {
                Assert.assertNull(data.getData());
                Assert.assertNotNull(data.getStat());
            }
        }

        cache.close();
    }

    @Test
    public void testBasics() throws Exception
    {
        Timing timing = new Timing();
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        client.start();
        try
        {
            client.create().forPath("/test");

            final BlockingQueue<PathChildrenCacheEvent.Type> events = new LinkedBlockingQueue<PathChildrenCacheEvent.Type>();
            PathChildrenCache cache = new PathChildrenCache(client, "/test", true);
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
            Assert.assertEquals(events.poll(timing.forWaiting().seconds(), TimeUnit.SECONDS), PathChildrenCacheEvent.Type.CHILD_ADDED);

            client.setData().forPath("/test/one", "sup!".getBytes());
            Assert.assertEquals(events.poll(timing.forWaiting().seconds(), TimeUnit.SECONDS), PathChildrenCacheEvent.Type.CHILD_UPDATED);
            Assert.assertEquals(new String(cache.getCurrentData("/test/one").getData()), "sup!");

            client.delete().forPath("/test/one");
            Assert.assertEquals(events.poll(timing.forWaiting().seconds(), TimeUnit.SECONDS), PathChildrenCacheEvent.Type.CHILD_REMOVED);

            cache.close();
        }
        finally
        {
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
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
            PathChildrenCache cache = new PathChildrenCache(client, "/test", true, false, exec);
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
            PathChildrenCache cache2 = new PathChildrenCache(client, "/test", true, false, exec);
            cache2.getListenable().addListener(
                    new PathChildrenCacheListener() {
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
            Assert.assertEquals(events.poll(timing.forWaiting().seconds(), TimeUnit.SECONDS), PathChildrenCacheEvent.Type.CHILD_ADDED);
            Assert.assertEquals(events2.poll(timing.forWaiting().seconds(), TimeUnit.SECONDS), PathChildrenCacheEvent.Type.CHILD_ADDED);

            client.setData().forPath("/test/one", "sup!".getBytes());
            Assert.assertEquals(events.poll(timing.forWaiting().seconds(), TimeUnit.SECONDS), PathChildrenCacheEvent.Type.CHILD_UPDATED);
            Assert.assertEquals(events2.poll(timing.forWaiting().seconds(), TimeUnit.SECONDS), PathChildrenCacheEvent.Type.CHILD_UPDATED);
            Assert.assertEquals(new String(cache.getCurrentData("/test/one").getData()), "sup!");
            Assert.assertEquals(new String(cache2.getCurrentData("/test/one").getData()), "sup!");

            client.delete().forPath("/test/one");
            Assert.assertEquals(events.poll(timing.forWaiting().seconds(), TimeUnit.SECONDS), PathChildrenCacheEvent.Type.CHILD_REMOVED);
            Assert.assertEquals(events2.poll(timing.forWaiting().seconds(), TimeUnit.SECONDS), PathChildrenCacheEvent.Type.CHILD_REMOVED);

            cache.close();
            cache2.close();
        }
        finally
        {
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
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
            PathChildrenCache cache = new PathChildrenCache(client, "/test", true, false, exec);

            cache.start();
            client.create().forPath("/test/one", "hey there".getBytes());

            cache.rebuild();
            Assert.assertEquals(new String(cache.getCurrentData("/test/one").getData()), "hey there");
            Assert.assertTrue(exec.isExecuteCalled());

            exec.setExecuteCalled(false);
            cache.close();
            Assert.assertFalse(exec.isExecuteCalled());

            client.delete().forPath("/test/one");
            timing.sleepABit();
            Assert.assertFalse(exec.isExecuteCalled());
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
    @Test
    public void testInterruptedOperationOnShutdown() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), 30000, 30000, new RetryOneTime(1));
        client.start();

        try
        {
            final CountDownLatch latch = new CountDownLatch(1);
            final PathChildrenCache cache = new PathChildrenCache(client, "/test", false) {
                @Override
                protected void handleException(Throwable e)
                {
                    latch.countDown();
                }
            };
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

            cache.close();

            latch.await(5, TimeUnit.SECONDS);

            Assert.assertTrue(latch.getCount() == 1, "Unexpected exception occurred");
        }
        finally
        {
            TestCleanState.closeAndTestClean(client);
        }
    }

    public static class ExecuteCalledWatchingExecutorService extends DelegatingExecutorService
    {
        boolean executeCalled = false;

        public ExecuteCalledWatchingExecutorService(ExecutorService delegate)
        {
            super(delegate);
        }

        @Override
        public synchronized void execute(Runnable command)
        {
            executeCalled = true;
            super.execute(command);
        }

        public synchronized boolean isExecuteCalled()
        {
            return executeCalled;
        }

        public synchronized void setExecuteCalled(boolean executeCalled)
        {
            this.executeCalled = executeCalled;
        }
    }

    public static class DelegatingExecutorService implements ExecutorService
    {
        private final ExecutorService delegate;

        public DelegatingExecutorService(
                ExecutorService delegate
        )
        {
            this.delegate = delegate;
        }


        @Override
        public void shutdown()
        {
            delegate.shutdown();
        }

        @Override
        public List<Runnable> shutdownNow()
        {
            return delegate.shutdownNow();
        }

        @Override
        public boolean isShutdown()
        {
            return delegate.isShutdown();
        }

        @Override
        public boolean isTerminated()
        {
            return delegate.isTerminated();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit)
                throws InterruptedException
        {
            return delegate.awaitTermination(timeout, unit);
        }

        @Override
        public <T> Future<T> submit(Callable<T> task)
        {
            return delegate.submit(task);
        }

        @Override
        public <T> Future<T> submit(Runnable task, T result)
        {
            return delegate.submit(task, result);
        }

        @Override
        public Future<?> submit(Runnable task)
        {
            return delegate.submit(task);
        }

        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
                throws InterruptedException
        {
            return delegate.invokeAll(tasks);
        }

        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
                throws InterruptedException
        {
            return delegate.invokeAll(tasks, timeout, unit);
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
                throws InterruptedException, ExecutionException
        {
            return delegate.invokeAny(tasks);
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
                throws InterruptedException, ExecutionException, TimeoutException
        {
            return delegate.invokeAny(tasks, timeout, unit);
        }

        @Override
        public void execute(Runnable command)
        {
            delegate.execute(command);
        }
    }
}
