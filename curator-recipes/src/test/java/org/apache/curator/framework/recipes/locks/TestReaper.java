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

package org.apache.curator.framework.recipes.locks;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

public class TestReaper extends BaseClassForTests
{
    @Test
    public void testUsingLeader() throws Exception
    {
        final Timing timing = new Timing();
        CuratorFramework client = makeClient(timing, null);
        Reaper reaper1 = null;
        Reaper reaper2 = null;
        try
        {
            final AtomicInteger reaper1Count = new AtomicInteger();
            reaper1 = new Reaper(client, Reaper.newExecutorService(), 1, "/reaper/leader")
            {
                @Override
                protected void reap(PathHolder holder)
                {
                    reaper1Count.incrementAndGet();
                    super.reap(holder);
                }
            };

            final AtomicInteger reaper2Count = new AtomicInteger();
            reaper2 = new Reaper(client, Reaper.newExecutorService(), 1, "/reaper/leader")
            {
                @Override
                protected void reap(PathHolder holder)
                {
                    reaper2Count.incrementAndGet();
                    super.reap(holder);
                }
            };

            client.start();
            client.create().creatingParentsIfNeeded().forPath("/one/two/three");

            reaper1.start();
            reaper2.start();

            reaper1.addPath("/one/two/three");
            reaper2.addPath("/one/two/three");

            timing.sleepABit();

            Assert.assertTrue((reaper1Count.get() == 0) || (reaper2Count.get() == 0));
            Assert.assertTrue((reaper1Count.get() > 0) || (reaper2Count.get() > 0));

            Reaper activeReaper;
            AtomicInteger inActiveReaperCount;
            if ( reaper1Count.get() > 0 )
            {
                activeReaper = reaper1;
                inActiveReaperCount = reaper2Count;
            }
            else
            {
                activeReaper = reaper2;
                inActiveReaperCount = reaper1Count;
            }
            Assert.assertEquals(inActiveReaperCount.get(), 0);
            activeReaper.close();
            timing.sleepABit();
            Assert.assertTrue(inActiveReaperCount.get() > 0);
        }
        finally
        {
            CloseableUtils.closeQuietly(reaper1);
            CloseableUtils.closeQuietly(reaper2);
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testUsingManualLeader() throws Exception
    {
        final Timing timing = new Timing();
        final CuratorFramework client = makeClient(timing, null);
        final CountDownLatch latch = new CountDownLatch(1);
        LeaderSelectorListener listener = new LeaderSelectorListener()
        {
            @Override
            public void takeLeadership(CuratorFramework client) throws Exception
            {
                Reaper reaper = new Reaper(client, 1);
                try
                {
                    reaper.addPath("/one/two/three", Reaper.Mode.REAP_UNTIL_DELETE);
                    reaper.start();

                    timing.sleepABit();
                    latch.countDown();
                }
                finally
                {
                    CloseableUtils.closeQuietly(reaper);
                }
            }

            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState)
            {
            }
        };
        LeaderSelector selector = new LeaderSelector(client, "/leader", listener);
        try
        {
            client.start();
            client.create().creatingParentsIfNeeded().forPath("/one/two/three");

            Assert.assertNotNull(client.checkExists().forPath("/one/two/three"));

            selector.start();
            timing.awaitLatch(latch);

            Assert.assertNull(client.checkExists().forPath("/one/two/three"));
        }
        finally
        {
            CloseableUtils.closeQuietly(selector);
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testSparseUseNoReap() throws Exception
    {
        final int THRESHOLD = 3000;

        Timing timing = new Timing();
        Reaper reaper = null;
        CuratorFramework client = makeClient(timing, null);
        try
        {
            client.start();
            client.create().creatingParentsIfNeeded().forPath("/one/two/three");

            Assert.assertNotNull(client.checkExists().forPath("/one/two/three"));

            final Queue<Reaper.PathHolder> holders = new ConcurrentLinkedQueue<Reaper.PathHolder>();
            final ExecutorService pool = Executors.newCachedThreadPool();
            ScheduledExecutorService service = new ScheduledThreadPoolExecutor(1);

            reaper = new Reaper(client, service, THRESHOLD)
            {
                @Override
                protected Future<Void> schedule(final PathHolder pathHolder, int reapingThresholdMs)
                {
                    holders.add(pathHolder);
                    final Future<?> f = super.schedule(pathHolder, reapingThresholdMs);
                    pool.submit(new Callable<Void>()
                        {
                            @Override
                            public Void call() throws Exception
                            {
                                f.get();
                                holders.remove(pathHolder);
                                return null;
                            }
                        }
                               );
                    return null;
                }
            };
            reaper.start();
            reaper.addPath("/one/two/three");

            long start = System.currentTimeMillis();
            boolean emptyCountIsCorrect = false;
            while ( ((System.currentTimeMillis() - start) < timing.forWaiting().milliseconds()) && !emptyCountIsCorrect )   // need to loop as the Holder can go in/out of the Reaper's DelayQueue
            {
                for ( Reaper.PathHolder holder : holders )
                {
                    if ( holder.path.endsWith("/one/two/three") )
                    {
                        emptyCountIsCorrect = (holder.emptyCount > 0);
                        break;
                    }
                }
                Thread.sleep(1);
            }
            Assert.assertTrue(emptyCountIsCorrect);

            client.create().forPath("/one/two/three/foo");

            Thread.sleep(2 * (THRESHOLD / Reaper.EMPTY_COUNT_THRESHOLD));
            Assert.assertNotNull(client.checkExists().forPath("/one/two/three"));
            client.delete().forPath("/one/two/three/foo");

            Thread.sleep(THRESHOLD);
            timing.sleepABit();

            Assert.assertNull(client.checkExists().forPath("/one/two/three"));
        }
        finally
        {
            CloseableUtils.closeQuietly(reaper);
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testReapUntilDelete() throws Exception
    {
        testReapUntilDelete(null);
    }

    @Test
    public void testReapUntilDeleteNamespace() throws Exception
    {
        testReapUntilDelete("test");
    }

    @Test
    public void testReapUntilGone() throws Exception
    {
        testReapUntilGone(null);
    }

    @Test
    public void testReapUntilGoneNamespace() throws Exception
    {
        testReapUntilGone("test");
    }

    @Test
    public void testRemove() throws Exception
    {
        testRemove(null);
    }

    @Test
    public void testRemoveNamespace() throws Exception
    {
        testRemove("test");
    }

    @Test
    public void testSimulationWithLocks() throws Exception
    {
        testSimulationWithLocks(null);
    }

    @Test
    public void testSimulationWithLocksNamespace() throws Exception
    {
        testSimulationWithLocks("test");
    }

    @Test
    public void testWithEphemerals() throws Exception
    {
        testWithEphemerals(null);
    }

    @Test
    public void testWithEphemeralsNamespace() throws Exception
    {
        testWithEphemerals("test");
    }

    @Test
    public void testBasic() throws Exception
    {
        testBasic(null);
    }

    @Test
    public void testBasicNamespace() throws Exception
    {
        testBasic("test");
    }

    private void testReapUntilDelete(String namespace) throws Exception
    {
        Timing timing = new Timing();
        Reaper reaper = null;
        CuratorFramework client = makeClient(timing, namespace);
        try
        {
            client.start();
            client.create().creatingParentsIfNeeded().forPath("/one/two/three");

            Assert.assertNotNull(client.checkExists().forPath("/one/two/three"));

            reaper = new Reaper(client, 100);
            reaper.start();

            reaper.addPath("/one/two/three", Reaper.Mode.REAP_UNTIL_DELETE);
            timing.sleepABit();

            Assert.assertNull(client.checkExists().forPath("/one/two/three"));

            client.create().forPath("/one/two/three");
            timing.sleepABit();
            Assert.assertNotNull(client.checkExists().forPath("/one/two/three"));
        }
        finally
        {
            CloseableUtils.closeQuietly(reaper);
            CloseableUtils.closeQuietly(client);
        }
    }

    private void testReapUntilGone(String namespace) throws Exception
    {
        Timing timing = new Timing();
        Reaper reaper = null;
        CuratorFramework client = makeClient(timing, namespace);
        try
        {
            client.start();

            reaper = new Reaper(client, 100);
            reaper.start();

            reaper.addPath("/one/two/three", Reaper.Mode.REAP_UNTIL_GONE);
            timing.sleepABit();

            client.create().creatingParentsIfNeeded().forPath("/one/two/three");
            Assert.assertNotNull(client.checkExists().forPath("/one/two/three"));

            reaper.addPath("/one/two/three", Reaper.Mode.REAP_UNTIL_GONE);
            timing.sleepABit();

            Assert.assertNull(client.checkExists().forPath("/one/two/three"));
        }
        finally
        {
            CloseableUtils.closeQuietly(reaper);
            CloseableUtils.closeQuietly(client);
        }
    }

    private CuratorFramework makeClient(Timing timing, String namespace) throws IOException
    {
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder().connectionTimeoutMs(timing.connection()).sessionTimeoutMs(timing.session()).connectString(server.getConnectString()).retryPolicy(new RetryOneTime(1));
        if ( namespace != null )
        {
            builder = builder.namespace(namespace);
        }
        return builder.build();
    }

    private void testRemove(String namespace) throws Exception
    {
        Timing timing = new Timing();
        Reaper reaper = null;
        CuratorFramework client = makeClient(timing, namespace);
        try
        {
            client.start();
            client.create().creatingParentsIfNeeded().forPath("/one/two/three");

            Assert.assertNotNull(client.checkExists().forPath("/one/two/three"));

            reaper = new Reaper(client, 100);
            reaper.start();

            reaper.addPath("/one/two/three");
            timing.sleepABit();

            Assert.assertNull(client.checkExists().forPath("/one/two/three"));

            Assert.assertTrue(reaper.removePath("/one/two/three"));

            client.create().forPath("/one/two/three");
            timing.sleepABit();
            Assert.assertNotNull(client.checkExists().forPath("/one/two/three"));
        }
        finally
        {
            CloseableUtils.closeQuietly(reaper);
            CloseableUtils.closeQuietly(client);
        }
    }

    private void testSimulationWithLocks(String namespace) throws Exception
    {
        final int LOCK_CLIENTS = 10;
        final int ITERATIONS = 250;
        final int MAX_WAIT_MS = 10;

        ExecutorService service = Executors.newFixedThreadPool(LOCK_CLIENTS);
        ExecutorCompletionService<Object> completionService = new ExecutorCompletionService<Object>(service);

        Timing timing = new Timing();
        Reaper reaper = null;
        final CuratorFramework client = makeClient(timing, namespace);
        try
        {
            client.start();

            reaper = new Reaper(client, MAX_WAIT_MS / 2);
            reaper.start();
            reaper.addPath("/a/b");

            for ( int i = 0; i < LOCK_CLIENTS; ++i )
            {
                completionService.submit(new Callable<Object>()
                    {
                        @Override
                        public Object call() throws Exception
                        {
                            final InterProcessMutex lock = new InterProcessMutex(client, "/a/b");
                            for ( int i = 0; i < ITERATIONS; ++i )
                            {
                                lock.acquire();
                                try
                                {
                                    Thread.sleep((int)(Math.random() * MAX_WAIT_MS));
                                }
                                finally
                                {
                                    lock.release();
                                }
                            }
                            return null;
                        }
                    }
                                        );
            }

            for ( int i = 0; i < LOCK_CLIENTS; ++i )
            {
                completionService.take().get();
            }

            Thread.sleep(timing.session());
            timing.sleepABit();

            Stat stat = client.checkExists().forPath("/a/b");
            Assert.assertNull(stat, "Child qty: " + ((stat != null) ? stat.getNumChildren() : 0));
        }
        finally
        {
            service.shutdownNow();
            CloseableUtils.closeQuietly(reaper);
            CloseableUtils.closeQuietly(client);
        }
    }

    private void testWithEphemerals(String namespace) throws Exception
    {
        Timing timing = new Timing();
        Reaper reaper = null;
        CuratorFramework client2 = null;
        CuratorFramework client = makeClient(timing, namespace);
        try
        {
            client.start();
            client.create().creatingParentsIfNeeded().forPath("/one/two/three");

            Assert.assertNotNull(client.checkExists().forPath("/one/two/three"));

            client2 = makeClient(timing, namespace);
            client2.start();
            for ( int i = 0; i < 10; ++i )
            {
                client2.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath("/one/two/three/foo-");
            }

            reaper = new Reaper(client, 100);
            reaper.start();

            reaper.addPath("/one/two/three");
            timing.sleepABit();

            Assert.assertNotNull(client.checkExists().forPath("/one/two/three"));

            client2.close();    // should clear ephemerals
            client2 = null;

            Thread.sleep(timing.session());
            timing.sleepABit();

            Assert.assertNull(client.checkExists().forPath("/one/two/three"));
        }
        finally
        {
            CloseableUtils.closeQuietly(reaper);
            CloseableUtils.closeQuietly(client2);
            CloseableUtils.closeQuietly(client);
        }
    }

    private void testBasic(String namespace) throws Exception
    {
        Timing timing = new Timing();
        Reaper reaper = null;
        CuratorFramework client = makeClient(timing, namespace);
        try
        {
            client.start();
            client.create().creatingParentsIfNeeded().forPath("/one/two/three");

            Assert.assertNotNull(client.checkExists().forPath("/one/two/three"));

            reaper = new Reaper(client, 100);
            reaper.start();

            reaper.addPath("/one/two/three");
            timing.sleepABit();

            Assert.assertNull(client.checkExists().forPath("/one/two/three"));
        }
        finally
        {
            CloseableUtils.closeQuietly(reaper);
            CloseableUtils.closeQuietly(client);
        }
    }
}
