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

package org.apache.curator.framework.recipes.leader;

import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import org.apache.curator.connection.StandardConnectionHandlingPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.framework.state.SessionErrorPolicy;
import org.apache.curator.framework.state.StandardErrorPolicy;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.KillSession;
import org.apache.curator.test.TestingServer;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.internal.annotations.Sets;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.fail;

public class TestLeaderSelector extends BaseClassForTests
{
    private static final String PATH_NAME = "/one/two/me";

    @Test
    public void testErrorPolicies() throws Exception
    {
        Timing timing = new Timing();
        LeaderSelector selector = null;
        CuratorFramework client = CuratorFrameworkFactory
            .builder()
            .connectString(server.getConnectString())
            .connectionTimeoutMs(timing.connection())
            .sessionTimeoutMs(timing.session())
            .retryPolicy(new RetryOneTime(1))
            .errorPolicy(new StandardErrorPolicy())
            .build();
        try
        {
            final BlockingQueue<String> changes = Queues.newLinkedBlockingQueue();

            ConnectionStateListener stateListener = new ConnectionStateListener()
            {
                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState)
                {
                    changes.add(newState.name());
                }
            };
            client.getConnectionStateListenable().addListener(stateListener);
            client.start();
            LeaderSelectorListener listener = new LeaderSelectorListenerAdapter()
            {
                @Override
                public void takeLeadership(CuratorFramework client) throws Exception
                {
                    changes.add("leader");
                    try
                    {
                        Thread.currentThread().join();
                    }
                    catch ( InterruptedException e )
                    {
                        changes.add("release");
                        Thread.currentThread().interrupt();
                    }
                }
            };
            selector = new LeaderSelector(client, "/test", listener);
            selector.start();

            Assert.assertEquals(changes.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.CONNECTED.name());
            Assert.assertEquals(changes.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS), "leader");
            server.close();
            Assert.assertEquals(changes.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.SUSPENDED.name());
            Assert.assertEquals(changes.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS), "release");
            Assert.assertEquals(changes.poll(timing.forSessionSleep().milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.LOST.name());

            selector.close();
            client.close();
            timing.sleepABit();
            changes.clear();

            server = new TestingServer();
            client = CuratorFrameworkFactory
                .builder()
                .connectString(server.getConnectString())
                .connectionTimeoutMs(timing.connection())
                .sessionTimeoutMs(timing.session())
                .retryPolicy(new RetryOneTime(1))
                .errorPolicy(new SessionErrorPolicy())
                .build();
            client.getConnectionStateListenable().addListener(stateListener);
            client.start();
            selector = new LeaderSelector(client, "/test", listener);
            selector.start();

            Assert.assertEquals(changes.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.CONNECTED.name());
            Assert.assertEquals(changes.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS), "leader");
            server.stop();
            Assert.assertEquals(changes.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.SUSPENDED.name());
            Assert.assertEquals(changes.poll(timing.forSessionSleep().milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.LOST.name());
            Assert.assertEquals(changes.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS), "release");
        }
        finally
        {
            CloseableUtils.closeQuietly(selector);
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testLeaderNodeDeleteOnInterrupt() throws Exception
    {
        Timing timing = new Timing();
        LeaderSelector selector = null;
        CuratorFramework client = null;
        try
        {
            client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
            final CountDownLatch reconnectedLatch = new CountDownLatch(1);
            ConnectionStateListener connectionStateListener = new ConnectionStateListener()
            {
                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState)
                {
                    if ( newState == ConnectionState.RECONNECTED )
                    {
                        reconnectedLatch.countDown();
                    }
                }
            };
            client.getConnectionStateListenable().addListener(connectionStateListener);
            client.start();

            final BlockingQueue<Thread> queue = new ArrayBlockingQueue<Thread>(1);
            LeaderSelectorListener listener = new LeaderSelectorListener()
            {
                @Override
                public void takeLeadership(CuratorFramework client) throws Exception
                {
                    queue.add(Thread.currentThread());
                    try
                    {
                        Thread.currentThread().join();
                    }
                    catch ( InterruptedException e )
                    {
                        Thread.currentThread().interrupt();
                    }
                }

                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState)
                {
                }
            };
            selector = new LeaderSelector(client, "/leader", listener);
            selector.start();

            Thread leaderThread = queue.take();
            server.stop();
            leaderThread.interrupt();
            server.restart();
            Assert.assertTrue(timing.awaitLatch(reconnectedLatch));
            timing.sleepABit();

            Assert.assertEquals(client.getChildren().forPath("/leader").size(), 0);
        }
        finally
        {
            CloseableUtils.closeQuietly(selector);
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testInterruptLeadershipWithRequeue() throws Exception
    {
        Timing timing = new Timing();
        LeaderSelector selector = null;
        CuratorFramework client = null;
        try
        {
            client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
            client.start();

            final Semaphore semaphore = new Semaphore(0);
            LeaderSelectorListener listener = new LeaderSelectorListenerAdapter()
            {
                @Override
                public void takeLeadership(CuratorFramework client) throws Exception
                {
                    semaphore.release();
                    Thread.currentThread().join();
                }
            };
            selector = new LeaderSelector(client, "/leader", listener);
            selector.autoRequeue();
            selector.start();

            Assert.assertTrue(timing.acquireSemaphore(semaphore));
            selector.interruptLeadership();

            Assert.assertTrue(timing.acquireSemaphore(semaphore));
        }
        finally
        {
            CloseableUtils.closeQuietly(selector);
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testInterruptLeadership() throws Exception
    {
        LeaderSelector selector = null;
        Timing timing = new Timing();
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        try
        {
            client.start();

            final CountDownLatch isLeaderLatch = new CountDownLatch(1);
            final CountDownLatch losingLeaderLatch = new CountDownLatch(1);
            LeaderSelectorListener listener = new LeaderSelectorListener()
            {
                @Override
                public void takeLeadership(CuratorFramework client) throws Exception
                {
                    isLeaderLatch.countDown();
                    try
                    {
                        Thread.currentThread().join();
                    }
                    finally
                    {
                        losingLeaderLatch.countDown();
                    }
                }

                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState)
                {
                }
            };

            selector = new LeaderSelector(client, "/leader", listener);
            selector.start();

            Assert.assertTrue(timing.awaitLatch(isLeaderLatch));
            selector.interruptLeadership();
            Assert.assertTrue(timing.awaitLatch(losingLeaderLatch));
        }
        finally
        {
            CloseableUtils.closeQuietly(selector);
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testRaceAtStateChanged() throws Exception
    {
        LeaderSelector selector = null;
        Timing timing = new Timing();
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        try
        {
            client.start();

            final CountDownLatch takeLeadershipLatch = new CountDownLatch(1);
            final CountDownLatch lostLatch = new CountDownLatch(1);
            final CountDownLatch reconnectedLatch = new CountDownLatch(1);
            LeaderSelectorListener listener = new LeaderSelectorListener()
            {
                @Override
                public void takeLeadership(CuratorFramework client) throws Exception
                {
                    takeLeadershipLatch.countDown();  // should never get here
                }

                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState)
                {
                    if ( newState == ConnectionState.RECONNECTED )
                    {
                        reconnectedLatch.countDown();
                    }
                    else if ( newState == ConnectionState.LOST )
                    {
                        lostLatch.countDown();
                        throw new CancelLeadershipException();
                    }
                }
            };

            selector = new LeaderSelector(client, "/leader", listener);
            CountDownLatch debugLeadershipLatch = new CountDownLatch(1);
            CountDownLatch debugLeadershipWaitLatch = new CountDownLatch(1);
            selector.debugLeadershipLatch = debugLeadershipLatch;
            selector.debugLeadershipWaitLatch = debugLeadershipWaitLatch;

            selector.start();

            Assert.assertTrue(timing.awaitLatch(debugLeadershipLatch));
            server.stop();
            Assert.assertTrue(timing.awaitLatch(lostLatch));
            timing.sleepABit();
            debugLeadershipWaitLatch.countDown();

            server.restart();
            Assert.assertTrue(timing.awaitLatch(reconnectedLatch));

            Assert.assertFalse(takeLeadershipLatch.await(3, TimeUnit.SECONDS));
        }
        finally
        {
            CloseableUtils.closeQuietly(selector);
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testAutoRequeue() throws Exception
    {
        Timing timing = new Timing();
        LeaderSelector selector = null;
        CuratorFramework client = CuratorFrameworkFactory.builder().connectString(server.getConnectString()).retryPolicy(new RetryOneTime(1)).sessionTimeoutMs(timing.session()).build();
        try
        {
            client.start();

            final Semaphore semaphore = new Semaphore(0);
            LeaderSelectorListener listener = new LeaderSelectorListener()
            {
                @Override
                public void takeLeadership(CuratorFramework client) throws Exception
                {
                    Thread.sleep(10);
                    semaphore.release();
                }

                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState)
                {
                }
            };
            selector = new LeaderSelector(client, "/leader", listener);
            selector.autoRequeue();
            selector.start();

            Assert.assertTrue(timing.acquireSemaphore(semaphore, 2));
        }
        finally
        {
            CloseableUtils.closeQuietly(selector);
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testServerDying() throws Exception
    {
        Timing timing = new Timing();
        LeaderSelector selector = null;
        CuratorFramework client = CuratorFrameworkFactory.builder().connectionTimeoutMs(timing.connection()).connectString(server.getConnectString()).retryPolicy(new RetryOneTime(1)).sessionTimeoutMs(timing.session()).build();
        client.start();
        try
        {
            final Semaphore semaphore = new Semaphore(0);
            LeaderSelectorListener listener = new LeaderSelectorListener()
            {
                @Override
                public void takeLeadership(CuratorFramework client) throws Exception
                {
                    semaphore.release();
                    Thread.sleep(Integer.MAX_VALUE);
                }

                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState)
                {
                    if ( newState == ConnectionState.LOST )
                    {
                        semaphore.release();
                    }
                }
            };
            selector = new LeaderSelector(client, "/leader", listener);
            selector.start();

            timing.acquireSemaphore(semaphore);

            server.close();

            timing.acquireSemaphore(semaphore);
        }
        finally
        {
            CloseableUtils.closeQuietly(selector);
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testKillSessionThenCloseShouldElectNewLeader() throws Exception
    {
        final Timing timing = new Timing();

        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        client.start();
        try
        {
            final Semaphore semaphore = new Semaphore(0);
            final CountDownLatch interruptedLatch = new CountDownLatch(1);
            final AtomicInteger leaderCount = new AtomicInteger(0);
            LeaderSelectorListener listener = new LeaderSelectorListenerAdapter()
            {
                @Override
                public void takeLeadership(CuratorFramework client) throws Exception
                {
                    leaderCount.incrementAndGet();
                    try
                    {
                        semaphore.release();
                        try
                        {
                            Thread.currentThread().join();
                        }
                        catch ( InterruptedException e )
                        {
                            Thread.currentThread().interrupt();
                            interruptedLatch.countDown();
                        }
                    }
                    finally
                    {
                        leaderCount.decrementAndGet();
                    }
                }
            };
            LeaderSelector leaderSelector1 = new LeaderSelector(client, PATH_NAME, listener);
            LeaderSelector leaderSelector2 = new LeaderSelector(client, PATH_NAME, listener);

            boolean leaderSelector1Closed = false;
            boolean leaderSelector2Closed = false;

            leaderSelector1.start();
            leaderSelector2.start();

            Assert.assertTrue(timing.acquireSemaphore(semaphore, 1));

            KillSession.kill(client.getZookeeperClient().getZooKeeper(), server.getConnectString());

            Assert.assertTrue(timing.awaitLatch(interruptedLatch));
            timing.sleepABit();

            boolean requeued1 = leaderSelector1.requeue();
            boolean requeued2 = leaderSelector2.requeue();
            Assert.assertTrue(requeued1);
            Assert.assertTrue(requeued2);

            Assert.assertTrue(timing.acquireSemaphore(semaphore, 1));
            Assert.assertEquals(leaderCount.get(), 1);

            if ( leaderSelector1.hasLeadership() )
            {
                leaderSelector1.close();
                leaderSelector1Closed = true;
            }
            else if ( leaderSelector2.hasLeadership() )
            {
                leaderSelector2.close();
                leaderSelector2Closed = true;
            }
            else
            {
                fail("No leaderselector has leadership!");
            }

            // Verify that the other leader took over leadership.
            Assert.assertTrue(timing.acquireSemaphore(semaphore, 1));
            Assert.assertEquals(leaderCount.get(), 1);

            if ( !leaderSelector1Closed )
            {
                leaderSelector1.close();
            }
            if ( !leaderSelector2Closed )
            {
                leaderSelector2.close();
            }
        }
        finally
        {
            client.close();
        }
    }

    /**
     * This is similar to TestLeaderSelector.testKillSessionThenCloseShouldElectNewLeader
     * The differences are:
     * it restarts the TestingServer instead of killing the session
     * it uses autoRequeue instead of explicitly calling requeue
     */
    @Test
    public void testKillServerThenCloseShouldElectNewLeader() throws Exception
    {
        final Timing timing = new Timing();

        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        client.start();
        try
        {
            final Semaphore semaphore = new Semaphore(0);
            final CountDownLatch interruptedLatch = new CountDownLatch(1);
            final AtomicInteger leaderCount = new AtomicInteger(0);
            LeaderSelectorListener listener = new LeaderSelectorListenerAdapter()
            {
                @Override
                public void takeLeadership(CuratorFramework client) throws Exception
                {
                    leaderCount.incrementAndGet();
                    try
                    {
                        semaphore.release();
                        try
                        {
                            Thread.currentThread().join();
                        }
                        catch ( InterruptedException e )
                        {
                            Thread.currentThread().interrupt();
                            interruptedLatch.countDown();
                        }
                    }
                    finally
                    {
                        leaderCount.decrementAndGet();
                    }
                }
            };
            LeaderSelector leaderSelector1 = new LeaderSelector(client, PATH_NAME, listener);
            LeaderSelector leaderSelector2 = new LeaderSelector(client, PATH_NAME, listener);

            boolean leaderSelector1Closed = false;
            boolean leaderSelector2Closed = false;

            leaderSelector1.autoRequeue();
            leaderSelector2.autoRequeue();

            leaderSelector1.start();
            leaderSelector2.start();

            Assert.assertTrue(timing.acquireSemaphore(semaphore, 1));

            int port = server.getPort();
            server.stop();
            timing.sleepABit();
            server = new TestingServer(port);
            Assert.assertTrue(timing.awaitLatch(interruptedLatch));
            timing.sleepABit();

            Assert.assertTrue(timing.acquireSemaphore(semaphore, 1));
            Assert.assertEquals(leaderCount.get(), 1);

            if ( leaderSelector1.hasLeadership() )
            {
                leaderSelector1.close();
                leaderSelector1Closed = true;
            }
            else if ( leaderSelector2.hasLeadership() )
            {
                leaderSelector2.close();
                leaderSelector2Closed = true;
            }
            else
            {
                fail("No leaderselector has leadership!");
            }

            // Verify that the other leader took over leadership.
            Assert.assertTrue(timing.acquireSemaphore(semaphore, 1));
            Assert.assertEquals(leaderCount.get(), 1);

            if ( !leaderSelector1Closed )
            {
                leaderSelector1.close();
            }
            if ( !leaderSelector2Closed )
            {
                leaderSelector2.close();
            }
        }
        finally
        {
            client.close();
        }
    }

    @Test
    public void testClosing() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            final CountDownLatch latch = new CountDownLatch(1);
            LeaderSelector leaderSelector1 = new LeaderSelector(client, PATH_NAME, new LeaderSelectorListener()
            {
                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState)
                {
                }

                @Override
                public void takeLeadership(CuratorFramework client) throws Exception
                {
                    latch.await(10, TimeUnit.SECONDS);
                }
            });

            LeaderSelector leaderSelector2 = new LeaderSelector(client, PATH_NAME, new LeaderSelectorListener()
            {
                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState)
                {
                }

                @Override
                public void takeLeadership(CuratorFramework client) throws Exception
                {
                    latch.await(10, TimeUnit.SECONDS);
                }
            });

            leaderSelector1.start();
            leaderSelector2.start();

            while ( !leaderSelector1.hasLeadership() && !leaderSelector2.hasLeadership() )
            {
                Thread.sleep(1000);
            }

            Assert.assertNotSame(leaderSelector1.hasLeadership(), leaderSelector2.hasLeadership());

            LeaderSelector positiveLeader;
            LeaderSelector negativeLeader;
            if ( leaderSelector1.hasLeadership() )
            {
                positiveLeader = leaderSelector1;
                negativeLeader = leaderSelector2;
            }
            else
            {
                positiveLeader = leaderSelector2;
                negativeLeader = leaderSelector1;
            }

            negativeLeader.close();
            Thread.sleep(1000);
            Assert.assertNotSame(positiveLeader.hasLeadership(), negativeLeader.hasLeadership());
            Assert.assertTrue(positiveLeader.hasLeadership());

            positiveLeader.close();
            Thread.sleep(1000);
            Assert.assertFalse(positiveLeader.hasLeadership());
        }
        finally
        {
            client.close();
        }
    }

    @SuppressWarnings({"ForLoopReplaceableByForEach"})
    @Test
    public void testRotatingLeadership() throws Exception
    {
        final int LEADER_QTY = 5;
        final int REPEAT_QTY = 3;

        final Timing timing = new Timing();
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        client.start();
        try
        {
            final BlockingQueue<Integer> leaderList = new LinkedBlockingQueue<Integer>();
            List<LeaderSelector> selectors = Lists.newArrayList();
            for ( int i = 0; i < LEADER_QTY; ++i )
            {
                final int ourIndex = i;
                LeaderSelector leaderSelector = new LeaderSelector(client, PATH_NAME, new LeaderSelectorListener()
                {
                    @Override
                    public void takeLeadership(CuratorFramework client) throws Exception
                    {
                        timing.sleepABit();
                        leaderList.add(ourIndex);
                    }

                    @Override
                    public void stateChanged(CuratorFramework client, ConnectionState newState)
                    {
                    }
                });
                selectors.add(leaderSelector);
            }

            List<Integer> localLeaderList = Lists.newArrayList();
            for ( int i = 1; i <= REPEAT_QTY; ++i )
            {
                for ( LeaderSelector leaderSelector : selectors )
                {
                    if ( i > 1 )
                    {
                        leaderSelector.requeue();
                    }
                    else
                    {
                        leaderSelector.start();
                    }
                }

                while ( localLeaderList.size() != (i * selectors.size()) )
                {
                    Integer polledIndex = leaderList.poll(10, TimeUnit.SECONDS);
                    Assert.assertNotNull(polledIndex);
                    localLeaderList.add(polledIndex);
                }
                timing.sleepABit();
            }

            for ( LeaderSelector leaderSelector : selectors )
            {
                leaderSelector.close();
            }
            System.out.println(localLeaderList);

            for ( int i = 0; i < REPEAT_QTY; ++i )
            {
                Set<Integer> uniques = Sets.newHashSet();
                for ( int j = 0; j < selectors.size(); ++j )
                {
                    Assert.assertTrue(localLeaderList.size() > 0);

                    int thisIndex = localLeaderList.remove(0);
                    Assert.assertFalse(uniques.contains(thisIndex));
                    uniques.add(thisIndex);
                }
            }
        }
        finally
        {
            client.close();
        }
    }
}
