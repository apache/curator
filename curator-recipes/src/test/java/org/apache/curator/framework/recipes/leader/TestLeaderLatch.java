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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.TestCleanState;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.framework.state.ConnectionStateListenerManagerFactory;
import org.apache.curator.framework.state.SessionConnectionStateErrorPolicy;
import org.apache.curator.framework.state.StandardConnectionStateErrorPolicy;
import org.apache.curator.retry.RetryForever;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.TestingServer;
import org.apache.curator.test.Timing;
import org.apache.curator.test.compatibility.CuratorTestBase;
import org.apache.curator.test.compatibility.Timing2;
import org.apache.curator.utils.CloseableUtils;
import org.awaitility.Awaitility;
import org.awaitility.core.ThrowingRunnable;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Tag(CuratorTestBase.zk35TestCompatibilityGroup)
public class TestLeaderLatch extends BaseClassForTests
{
    private static final String PATH_NAME = "/one/two/me";
    private static final int MAX_LOOPS = 5;

    private static class Holder
    {
        final BlockingQueue<ConnectionState> stateChanges = new LinkedBlockingQueue<>();
        final CountDownLatch isLockedLatch = new CountDownLatch(1);
        volatile LeaderLatch latch;
    }

    @Test
    public void testWithCircuitBreaker() throws Exception
    {
        final int threadQty = 5;

        ExecutorService executorService = Executors.newFixedThreadPool(threadQty);
        List<Holder> holders = Collections.emptyList();
        Timing2 timing = new Timing2();
        ConnectionStateListenerManagerFactory managerFactory = ConnectionStateListenerManagerFactory.circuitBreaking(new RetryForever(timing.multiple(2).milliseconds()));
        CuratorFramework client = CuratorFrameworkFactory.builder()
            .connectString(server.getConnectString())
            .retryPolicy(new RetryOneTime(1))
            .connectionStateListenerManagerFactory(managerFactory)
            .connectionTimeoutMs(timing.connection())
            .sessionTimeoutMs(timing.session())
            .build();
        try {
            client.start();
            client.create().forPath("/hey");

            Semaphore lostSemaphore = new Semaphore(0);
            ConnectionStateListener unProxiedListener = new ConnectionStateListener()
            {
                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState)
                {
                    if ( newState == ConnectionState.LOST )
                    {
                        lostSemaphore.release();
                    }
                }

                @Override
                public boolean doNotProxy()
                {
                    return true;
                }
            };
            client.getConnectionStateListenable().addListener(unProxiedListener);

            holders = IntStream.range(0, threadQty)
                .mapToObj(index -> {
                    Holder holder = new Holder();
                    holder.latch = new LeaderLatch(client, "/foo/bar/" + index)
                    {
                        @Override
                        protected void handleStateChange(ConnectionState newState)
                        {
                            holder.stateChanges.offer(newState);
                            super.handleStateChange(newState);
                        }
                    };
                    return holder;
                })
                .collect(Collectors.toList());

            holders.forEach(holder -> {
                executorService.submit(() -> {
                    holder.latch.start();
                    assertTrue(holder.latch.await(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS));
                    holder.isLockedLatch.countDown();
                    return null;
                });
                timing.awaitLatch(holder.isLockedLatch);
            });

            for ( int i = 0; i < 4; ++i )   // note: 4 is just a random number of loops to simulate disconnections
            {
                server.stop();
                assertTrue(timing.acquireSemaphore(lostSemaphore));
                server.restart();
                timing.sleepABit();
            }

            for ( Holder holder : holders )
            {
                assertTrue(holder.latch.await(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS));
                assertEquals(timing.takeFromQueue(holder.stateChanges), ConnectionState.SUSPENDED);
                assertEquals(timing.takeFromQueue(holder.stateChanges), ConnectionState.LOST);
                assertEquals(timing.takeFromQueue(holder.stateChanges), ConnectionState.RECONNECTED);
            }
        }
        finally
        {
            holders.forEach(holder -> CloseableUtils.closeQuietly(holder.latch));
            CloseableUtils.closeQuietly(client);
            executorService.shutdownNow();
        }
    }

    @Test
    public void testUncreatedPathGetLeader() throws Exception
    {
        try ( CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1)) )
        {
            client.start();
            LeaderLatch latch = new LeaderLatch(client, "/foo/bar");
            latch.getLeader();  // CURATOR-436 - was throwing NoNodeException
        }
    }

    @Test
    public void testWatchedNodeDeletedOnReconnect() throws Exception
    {
        final String latchPath = "/foo/bar";
        Timing2 timing = new Timing2();
        try ( CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1)) )
        {
            client.start();
            LeaderLatch latch1 = new LeaderLatch(client, latchPath, "1");
            try ( LeaderLatch latch2 = new LeaderLatch(client, latchPath, "2") )
            {
                latch1.start();
                assertTrue(latch1.await(timing.milliseconds(), TimeUnit.MILLISECONDS));

                latch2.start(); // will get a watcher on latch1's node
                timing.sleepABit();

                latch2.debugCheckLeaderShipLatch = new CountDownLatch(1);
                latch1.close();   // simulate the leader's path getting deleted
                latch1 = null;
                timing.sleepABit(); // after this, latch2 should be blocked just before getting the path in checkLeadership()

                latch2.reset(); // force the internal "ourPath" to get reset
                latch2.debugCheckLeaderShipLatch.countDown();   // allow checkLeadership() to continue

                assertTrue(latch2.await(timing.forSessionSleep().forWaiting().milliseconds(), TimeUnit.MILLISECONDS));
                timing.sleepABit();
                assertEquals(client.getChildren().forPath(latchPath).size(), 1);
            }
            finally
            {
                CloseableUtils.closeQuietly(latch1);
            }
        }
    }

    @Test
    public void testCheckLeaderShipTiming() throws Exception
    {
        final String latchPath = "/test";
        Timing timing = new Timing();
        List<LeaderLatch> latches = Lists.newArrayList();
        List<CuratorFramework> clients = Lists.newArrayList();
        final BlockingQueue<String> states = Queues.newLinkedBlockingQueue();
        for ( int i = 0; i < 2; ++i ) {
            try {
                CuratorFramework client = CuratorFrameworkFactory.builder()
                        .connectString(server.getConnectString())
                        .connectionTimeoutMs(10000)
                        .sessionTimeoutMs(60000)
                        .retryPolicy(new RetryOneTime(1))
                        .connectionStateErrorPolicy(new StandardConnectionStateErrorPolicy())
                        .build();
                ConnectionStateListener stateListener = new ConnectionStateListener()
                {
                    @Override
                    public void stateChanged(CuratorFramework client, ConnectionState newState)
                    {
                        if (newState == ConnectionState.CONNECTED) {
                            states.add(newState.name());
                        }
                    }
                };
                client.getConnectionStateListenable().addListener(stateListener);
                client.start();
                clients.add(client);
                LeaderLatch latch = new LeaderLatch(client, latchPath, String.valueOf(i));
                LeaderLatchListener listener = new LeaderLatchListener() {
                    @Override
                    public void isLeader() {
                        states.add("true");
                    }

                    @Override
                    public void notLeader() {
                        states.add("false");
                    }
                };
                latch.addListener(listener);
                latch.start();
                latches.add(latch);
                assertEquals(states.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.CONNECTED.name());
                if (i == 0) {
                    assertEquals(states.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS), "true");
                }
            }
            catch (Exception e){
                return;
            }
        }
        timing.forWaiting().sleepABit();
        // now latch1 is leader, latch2 is not leader. latch2 listens to the ephemeral node created by latch1
        LeaderLatch latch1 = latches.get(0);
        LeaderLatch latch2 = latches.get(1);
        assertTrue(latch1.hasLeadership());
        assertFalse(latch2.hasLeadership());
        try {
            latch2.debugResetWaitBeforeNodeDeleteLatch = new CountDownLatch(1);
            latch2.debugResetWaitLatch = new CountDownLatch(1);
            latch1.debugResetWaitLatch = new CountDownLatch(1);

            // force latch1 and latch2 reset
            latch1.reset();
            ForkJoinPool.commonPool().submit(() -> {
                latch2.reset();
                return null;
            });

            // latch1 set itself is not the leader state and will delete old path and create new path then wait before getChildren
            // latch2 wait before delete its old path and receive nodeDeleteEvent and then getChildren find itself is leader
            assertEquals(states.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS), "false"); //latch1 is not leader
            assertEquals(states.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS), "true");  //latch2 is leader
            assertTrue(latch2.hasLeadership());
            assertFalse(latch1.hasLeadership());
            // latch1 continue and getChildren and find itself is not the leader and listen to the node created by latch2
            latch1.debugResetWaitLatch.countDown();
            timing.sleepABit();
            // latch2 continue and delete old path and create new path then wait before getChildren
            latch2.debugResetWaitBeforeNodeDeleteLatch.countDown();
            // latch1 receive nodeDeleteEvent and then getChildren find itself is leader
            assertEquals(states.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS), "true");
            assertTrue(latch1.hasLeadership());
            latch2.debugResetWaitLatch.countDown(); // latch2 continue and getChildren find itself is not leader
            timing.forWaiting().sleepABit();

            assertTrue(latch1.hasLeadership());
            assertFalse(latch2.hasLeadership());
        }
        finally {
            for(int i = 0; i < clients.size(); ++i) {
                CloseableUtils.closeQuietly(latches.get(i));
                CloseableUtils.closeQuietly(clients.get(i));
            }
        }
    }

    @Test
    public void testLeadershipElectionWhenNodeDisappearsAfterChildrenAreRetrieved() throws Exception
    {
        final String latchPath = "/foo/bar";
        final Timing2 timing = new Timing2();
        final Duration pollInterval = Duration.ofMillis(100);
        try (CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1)))
        {
            client.start();
            LeaderLatch latchInitialLeader = new LeaderLatch(client, latchPath, "initial-leader");
            LeaderLatch latchCandidate0 = new LeaderLatch(client, latchPath, "candidate-0");
            LeaderLatch latchCandidate1 = new LeaderLatch(client, latchPath, "candidate-1");

            try
            {
                latchInitialLeader.start();

                // we want to make sure that the leader gets leadership before other instances are going to join the party
                waitForALeader(Collections.singletonList(latchInitialLeader), new Timing());
                // candidate #0 will wait for the leader to go away - this should happen after the child nodes are retrieved by candidate #0
                latchCandidate0.debugCheckLeaderShipLatch = new CountDownLatch(1);
                latchCandidate0.start();

                final int expectedChildrenAfterCandidate0Joins = 2;
                Awaitility.await("There should be " + expectedChildrenAfterCandidate0Joins + " child nodes created after candidate #0 joins the leader election.")
                        .pollInterval(pollInterval)
                        .pollInSameThread()
                        .until(() -> client.getChildren().forPath(latchPath).size() == expectedChildrenAfterCandidate0Joins);
                // no extra CountDownLatch needs to be set here because candidate #1 will rely on candidate #0
                latchCandidate1.start();

                final int expectedChildrenAfterCandidate1Joins = 3;
                Awaitility.await("There should be " + expectedChildrenAfterCandidate1Joins + " child nodes created after candidate #1 joins the leader election.")
                        .pollInterval(pollInterval)
                        .pollInSameThread()
                        .until(() -> client.getChildren().forPath(latchPath).size() == expectedChildrenAfterCandidate1Joins);

                // triggers the removal of the corresponding child node after candidate #0 retrieved the children
                latchInitialLeader.close();

                latchCandidate0.debugCheckLeaderShipLatch.countDown();

                waitForALeader(Arrays.asList(latchCandidate0, latchCandidate1), new Timing());

                assertTrue(latchCandidate0.hasLeadership() ^ latchCandidate1.hasLeadership());
            }
            finally
            {
                for (LeaderLatch latchToClose : Arrays.asList(latchInitialLeader, latchCandidate0, latchCandidate1))
                {
                    latchToClose.closeOnDemand();
                }
            }
        }
    }

    @Test
    public void testSessionErrorPolicy() throws Exception
    {
        Timing timing = new Timing();
        LeaderLatch latch = null;
        CuratorFramework client = null;
        for ( int i = 0; i < 2; ++i )
        {
            boolean isSessionIteration = (i == 0);
            try
            {
                client = CuratorFrameworkFactory.builder()
                    .connectString(server.getConnectString())
                    .connectionTimeoutMs(10000)
                    .sessionTimeoutMs(60000)
                    .retryPolicy(new RetryOneTime(1))
                    .connectionStateErrorPolicy(isSessionIteration ? new SessionConnectionStateErrorPolicy() : new StandardConnectionStateErrorPolicy())
                    .build();
                final BlockingQueue<String> states = Queues.newLinkedBlockingQueue();
                ConnectionStateListener stateListener = new ConnectionStateListener()
                {
                    @Override
                    public void stateChanged(CuratorFramework client, ConnectionState newState)
                    {
                        states.add(newState.name());
                    }
                };
                client.getConnectionStateListenable().addListener(stateListener);
                client.start();

                final String latchPatch = "/test";
                latch = new LeaderLatch(client, latchPatch);
                LeaderLatchListener listener = new LeaderLatchListener()
                {
                    @Override
                    public void isLeader()
                    {
                        states.add("true");
                    }

                    @Override
                    public void notLeader()
                    {
                        states.add("false");
                    }
                };
                latch.addListener(listener);
                latch.start();
                assertEquals(states.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.CONNECTED.name());
                assertEquals(states.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS), "true");
                final List<String> beforeResetChildren = client.getChildren().forPath(latchPatch);
                server.stop();
                if ( isSessionIteration )
                {
                    assertEquals(states.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.SUSPENDED.name());
                    server.restart();
                    assertEquals(states.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.RECONNECTED.name());
                    assertNull(states.poll(timing.milliseconds(), TimeUnit.MILLISECONDS));
                }
                else
                {
                    String s = states.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS);
                    assertTrue("false".equals(s) || ConnectionState.SUSPENDED.name().equals(s));
                    s = states.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS);
                    assertTrue("false".equals(s) || ConnectionState.SUSPENDED.name().equals(s));
                    server.restart();
                    assertEquals(states.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.RECONNECTED.name());
                    assertEquals(states.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS), "true");
                    final List<String> afterResetChildren = client.getChildren().forPath(latchPatch);
                    assertEquals(beforeResetChildren, afterResetChildren);
                }
            }
            finally
            {
                CloseableUtils.closeQuietly(latch);
                CloseableUtils.closeQuietly(client);
            }
        }
    }

    @Test
    public void testErrorPolicies() throws Exception
    {
        Timing2 timing = new Timing2();
        LeaderLatch latch = null;
        CuratorFramework client = CuratorFrameworkFactory.builder()
            .connectString(server.getConnectString())
            .connectionTimeoutMs(1000)
            .sessionTimeoutMs(timing.session())
            .retryPolicy(new RetryOneTime(1))
            .connectionStateErrorPolicy(new StandardConnectionStateErrorPolicy())
            .build();
        try
        {
            final BlockingQueue<String> states = Queues.newLinkedBlockingQueue();
            ConnectionStateListener stateListener = new ConnectionStateListener()
            {
                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState)
                {
                    states.add(newState.name());
                }
            };
            client.getConnectionStateListenable().addListener(stateListener);
            client.start();
            latch = new LeaderLatch(client, "/test");
            LeaderLatchListener listener = new LeaderLatchListener()
            {
                @Override
                public void isLeader()
                {
                    states.add("true");
                }

                @Override
                public void notLeader()
                {
                    states.add("false");
                }
            };
            latch.addListener(listener);
            latch.start();
            assertEquals(states.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.CONNECTED.name());
            assertEquals(states.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS), "true");
            server.close();
            List<String> next = Lists.newArrayList();
            next.add(states.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS));
            next.add(states.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS));
            assertTrue(next.equals(Arrays.asList(ConnectionState.SUSPENDED.name(), "false")) || next.equals(Arrays.asList("false", ConnectionState.SUSPENDED.name())), next.toString());
            assertEquals(states.poll(timing.forSessionSleep().milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.LOST.name());
            latch.close();
            client.close();

            timing.sleepABit();
            states.clear();

            server = new TestingServer();
            client = CuratorFrameworkFactory.builder()
                .connectString(server.getConnectString())
                .connectionTimeoutMs(1000)
                .sessionTimeoutMs(timing.session())
                .retryPolicy(new RetryOneTime(1))
                .connectionStateErrorPolicy(new SessionConnectionStateErrorPolicy())
                .build();
            client.getConnectionStateListenable().addListener(stateListener);
            client.start();
            latch = new LeaderLatch(client, "/test");
            latch.addListener(listener);
            latch.start();
            assertEquals(states.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.CONNECTED.name());
            assertEquals(states.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS), "true");
            server.close();
            assertEquals(states.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.SUSPENDED.name());
            next = Lists.newArrayList();
            next.add(states.poll(timing.forSessionSleep().milliseconds(), TimeUnit.MILLISECONDS));
            next.add(states.poll(timing.forSessionSleep().milliseconds(), TimeUnit.MILLISECONDS));
            assertTrue(next.equals(Arrays.asList(ConnectionState.LOST.name(), "false")) || next.equals(Arrays.asList("false", ConnectionState.LOST.name())), next.toString());
        }
        finally
        {
            CloseableUtils.closeQuietly(latch);
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testProperCloseWithoutConnectionEstablished() throws Exception
    {
        server.stop();

        Timing timing = new Timing();
        LeaderLatch latch = null;
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        try
        {
            client.start();

            final AtomicBoolean resetCalled = new AtomicBoolean(false);
            final CountDownLatch cancelStartTaskLatch = new CountDownLatch(1);
            latch = new LeaderLatch(client, PATH_NAME)
            {
                @Override
                void reset() throws Exception
                {
                    resetCalled.set(true);
                    super.reset();
                }

                @Override
                protected boolean cancelStartTask()
                {
                    if ( super.cancelStartTask() )
                    {
                        cancelStartTaskLatch.countDown();
                        return true;
                    }
                    return false;
                }
            };

            latch.start();
            latch.close();
            latch = null;

            assertTrue(timing.awaitLatch(cancelStartTaskLatch));
            assertFalse(resetCalled.get());
        }
        finally
        {
            CloseableUtils.closeQuietly(latch);
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
    public void testResetRace() throws Exception
    {
        Timing timing = new Timing();
        LeaderLatch latch = null;
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        try
        {
            client.start();
            latch = new LeaderLatch(client, PATH_NAME);

            latch.debugResetWaitLatch = new CountDownLatch(1);
            latch.start();  // will call reset()
            latch.reset();  // should not result in two nodes

            timing.sleepABit();

            latch.debugResetWaitLatch.countDown();

            timing.sleepABit();

            assertEquals(client.getChildren().forPath(PATH_NAME).size(), 1);
        }
        finally
        {
            CloseableUtils.closeQuietly(latch);
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
    public void testCreateDeleteRace() throws Exception
    {
        Timing timing = new Timing();
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        try
        {
            client.start();
            client.create().creatingParentsIfNeeded().forPath(PATH_NAME);

            LeaderLatch latch = new LeaderLatch(client, PATH_NAME);

            latch.debugResetWaitLatch = new CountDownLatch(1);

            latch.start();
            latch.close();

            timing.sleepABit();

            latch.debugResetWaitLatch.countDown();

            timing.sleepABit();

            assertEquals(client.getChildren().forPath(PATH_NAME).size(), 0);

        }
        finally
        {
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
    public void testLostConnection() throws Exception
    {
        final int PARTICIPANT_QTY = 10;

        List<LeaderLatch> latches = Lists.newArrayList();

        final Timing timing = new Timing();
        final CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        try
        {
            client.start();

            final CountDownLatch countDownLatch = new CountDownLatch(1);
            client.getConnectionStateListenable().addListener(new ConnectionStateListener()
            {
                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState)
                {
                    if ( newState == ConnectionState.LOST )
                    {
                        countDownLatch.countDown();
                    }
                }
            });

            for ( int i = 0; i < PARTICIPANT_QTY; ++i )
            {
                LeaderLatch latch = new LeaderLatch(client, PATH_NAME);
                latch.start();
                latches.add(latch);
            }

            waitForALeader(latches, timing);

            server.stop();
            assertTrue(timing.awaitLatch(countDownLatch));

            timing.forWaiting().sleepABit();

            assertEquals(getLeaders(latches).size(), 0);

            server.restart();
            assertEquals(waitForALeader(latches, timing).size(), 1); // should reconnect
        }
        finally
        {
            for ( LeaderLatch latch : latches )
            {
                CloseableUtils.closeQuietly(latch);
            }
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
    public void testCorrectWatching() throws Exception
    {
        final int PARTICIPANT_QTY = 10;
        final int PARTICIPANT_ID = 2;

        List<LeaderLatch> latches = Lists.newArrayList();

        final Timing timing = new Timing();
        final CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        try
        {
            client.start();

            for ( int i = 0; i < PARTICIPANT_QTY; ++i )
            {
                LeaderLatch latch = new LeaderLatch(client, PATH_NAME);
                latch.start();
                latches.add(latch);
                waitForALeader(latches, timing);
            }

            //we need to close a Participant that doesn't be actual leader (first Participant) nor the last
            latches.get(PARTICIPANT_ID).close();

            //As the previous algorithm assumed that if the watched node is deleted gets the leadership
            //we need to ensure that the PARTICIPANT_ID-1 is not getting (wrongly) elected as leader.
            assertTrue(!latches.get(PARTICIPANT_ID - 1).hasLeadership());
        }
        finally
        {
            //removes the already closed participant
            latches.remove(PARTICIPANT_ID);

            for ( LeaderLatch latch : latches )
            {
                CloseableUtils.closeQuietly(latch);
            }
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
    public void testWaiting() throws Exception
    {
        final int LOOPS = 10;
        for ( int i = 0; i < LOOPS; ++i )
        {
            System.out.println("TRY #" + i);
            internalTestWaitingOnce();
            Thread.sleep(10);
        }
    }

    private void internalTestWaitingOnce() throws Exception
    {
        final int PARTICIPANT_QTY = 10;

        ExecutorService executorService = Executors.newFixedThreadPool(PARTICIPANT_QTY);
        ExecutorCompletionService<Void> service = new ExecutorCompletionService<Void>(executorService);

        final Timing timing = new Timing();
        final CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        try
        {
            client.start();

            final AtomicBoolean thereIsALeader = new AtomicBoolean(false);
            for ( int i = 0; i < PARTICIPANT_QTY; ++i )
            {
                service.submit(new Callable<Void>()
                {
                    @Override
                    public Void call() throws Exception
                    {
                        LeaderLatch latch = new LeaderLatch(client, PATH_NAME);
                        try
                        {
                            latch.start();
                            assertTrue(latch.await(timing.forWaiting().seconds(), TimeUnit.SECONDS));
                            assertTrue(thereIsALeader.compareAndSet(false, true));
                            Thread.sleep((int)(10 * Math.random()));
                            thereIsALeader.set(false);
                        }
                        finally
                        {
                            latch.close();
                        }
                        return null;
                    }
                });
            }

            for ( int i = 0; i < PARTICIPANT_QTY; ++i )
            {
                service.take().get();
            }
        }
        finally
        {
            executorService.shutdownNow();
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
    public void testBasic() throws Exception
    {
        basic(Mode.START_IMMEDIATELY);
    }

    @Test
    public void testBasicAlt() throws Exception
    {
        basic(Mode.START_IN_THREADS);
    }

    @Test
    public void testCallbackSanity() throws Exception
    {
        final int PARTICIPANT_QTY = 10;
        final CountDownLatch timesSquare = new CountDownLatch(PARTICIPANT_QTY);
        final AtomicLong masterCounter = new AtomicLong(0);
        final AtomicLong notLeaderCounter = new AtomicLong(0);

        Timing timing = new Timing();
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        ExecutorService exec = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setDaemon(true).setNameFormat("callbackSanity-%s").build());

        List<LeaderLatch> latches = Lists.newArrayList();
        for ( int i = 0; i < PARTICIPANT_QTY; ++i )
        {
            final LeaderLatch latch = new LeaderLatch(client, PATH_NAME);
            latch.addListener(new LeaderLatchListener()
            {
                boolean beenLeader = false;

                @Override
                public void isLeader()
                {
                    if ( !beenLeader )
                    {
                        masterCounter.incrementAndGet();
                        beenLeader = true;
                        try
                        {
                            latch.reset();
                        }
                        catch ( Exception e )
                        {
                            throw Throwables.propagate(e);
                        }
                    }
                    else
                    {
                        masterCounter.incrementAndGet();
                        CloseableUtils.closeQuietly(latch);
                        timesSquare.countDown();
                    }
                }

                @Override
                public void notLeader()
                {
                    notLeaderCounter.incrementAndGet();
                }
            }, exec);
            latches.add(latch);
        }

        try
        {
            client.start();

            for ( LeaderLatch latch : latches )
            {
                latch.start();
            }

            timesSquare.await();

            assertEquals(masterCounter.get(), PARTICIPANT_QTY * 2);
            assertEquals(notLeaderCounter.get(), PARTICIPANT_QTY);
            for ( LeaderLatch latch : latches )
            {
                assertEquals(latch.getState(), LeaderLatch.State.CLOSED);
            }
        }
        finally
        {
            for ( LeaderLatch latch : latches )
            {
                if ( latch.getState() != LeaderLatch.State.CLOSED )
                {
                    CloseableUtils.closeQuietly(latch);
                }
            }
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
    public void testCallbackNotifyLeader() throws Exception
    {
        final int PARTICIPANT_QTY = 10;
        final int SILENT_QTY = 3;

        final CountDownLatch timesSquare = new CountDownLatch(PARTICIPANT_QTY);
        final AtomicLong masterCounter = new AtomicLong(0);
        final AtomicLong notLeaderCounter = new AtomicLong(0);

        Timing timing = new Timing();
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        ExecutorService exec = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setDaemon(true).setNameFormat("callbackNotifyLeader-%s").build());

        List<LeaderLatch> latches = Lists.newArrayList();
        for ( int i = 0; i < PARTICIPANT_QTY; ++i )
        {
            LeaderLatch.CloseMode closeMode = i < SILENT_QTY ? LeaderLatch.CloseMode.SILENT : LeaderLatch.CloseMode.NOTIFY_LEADER;

            final LeaderLatch latch = new LeaderLatch(client, PATH_NAME, "", closeMode);
            latch.addListener(new LeaderLatchListener()
            {
                boolean beenLeader = false;

                @Override
                public void isLeader()
                {
                    if ( !beenLeader )
                    {
                        masterCounter.incrementAndGet();
                        beenLeader = true;
                        try
                        {
                            latch.reset();
                        }
                        catch ( Exception e )
                        {
                            throw Throwables.propagate(e);
                        }
                    }
                    else
                    {
                        masterCounter.incrementAndGet();
                        CloseableUtils.closeQuietly(latch);
                        timesSquare.countDown();
                    }
                }

                @Override
                public void notLeader()
                {
                    notLeaderCounter.incrementAndGet();
                }
            }, exec);
            latches.add(latch);
        }

        try
        {
            client.start();

            for ( LeaderLatch latch : latches )
            {
                latch.start();
            }

            timesSquare.await();

            assertEquals(masterCounter.get(), PARTICIPANT_QTY * 2);
            assertEquals(notLeaderCounter.get(), PARTICIPANT_QTY * 2 - SILENT_QTY);
            for ( LeaderLatch latch : latches )
            {
                assertEquals(latch.getState(), LeaderLatch.State.CLOSED);
            }
        }
        finally
        {
            for ( LeaderLatch latch : latches )
            {
                if ( latch.getState() != LeaderLatch.State.CLOSED )
                {
                    CloseableUtils.closeQuietly(latch);
                }
            }
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
    public void testCallbackDontNotify() throws Exception
    {
        final AtomicLong masterCounter = new AtomicLong(0);
        final AtomicLong notLeaderCounter = new AtomicLong(0);

        Timing timing = new Timing();
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));

        final LeaderLatch leader = new LeaderLatch(client, PATH_NAME);
        final LeaderLatch notifiedLeader = new LeaderLatch(client, PATH_NAME, "", LeaderLatch.CloseMode.NOTIFY_LEADER);

        leader.addListener(new LeaderLatchListener()
        {
            @Override
            public void isLeader()
            {
            }

            @Override
            public void notLeader()
            {
                masterCounter.incrementAndGet();
            }
        });

        notifiedLeader.addListener(new LeaderLatchListener()
        {
            @Override
            public void isLeader()
            {
            }

            @Override
            public void notLeader()
            {
                notLeaderCounter.incrementAndGet();
            }
        });

        try
        {
            client.start();

            leader.start();

            timing.sleepABit();

            notifiedLeader.start();

            timing.sleepABit();

            notifiedLeader.close();

            timing.sleepABit();

            // Test the close override
            leader.close(LeaderLatch.CloseMode.NOTIFY_LEADER);

            assertEquals(leader.getState(), LeaderLatch.State.CLOSED);
            assertEquals(notifiedLeader.getState(), LeaderLatch.State.CLOSED);

            assertEquals(masterCounter.get(), 1);
            assertEquals(notLeaderCounter.get(), 0);
        }
        finally
        {
            if ( leader.getState() != LeaderLatch.State.CLOSED )
            {
                CloseableUtils.closeQuietly(leader);
            }
            if ( notifiedLeader.getState() != LeaderLatch.State.CLOSED )
            {
                CloseableUtils.closeQuietly(notifiedLeader);
            }
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
    public void testNoServerAtStart()
    {
        CloseableUtils.closeQuietly(server);

        Timing timing = new Timing();
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryNTimes(5, 1000));

        client.start();

        final LeaderLatch leader = new LeaderLatch(client, PATH_NAME);
        final CountDownLatch leaderCounter = new CountDownLatch(1);
        final AtomicInteger leaderCount = new AtomicInteger(0);
        final AtomicInteger notLeaderCount = new AtomicInteger(0);
        leader.addListener(new LeaderLatchListener()
        {
            @Override
            public void isLeader()
            {
                leaderCounter.countDown();
                leaderCount.incrementAndGet();
            }

            @Override
            public void notLeader()
            {
                notLeaderCount.incrementAndGet();
            }

        });

        try
        {
            leader.start();

            timing.sleepABit();

            // Start the new server
            server = new TestingServer(server.getPort(), server.getTempDirectory());

            assertTrue(timing.awaitLatch(leaderCounter), "Not elected leader");

            assertEquals(leaderCount.get(), 1, "Elected too many times");
            assertEquals(notLeaderCount.get(), 0, "Unelected too many times");
        }
        catch ( Exception e )
        {
            fail("Unexpected exception", e);
        }
        finally
        {
            CloseableUtils.closeQuietly(leader);
            TestCleanState.closeAndTestClean(client);
            CloseableUtils.closeQuietly(server);
        }
    }

    private enum Mode
    {
        START_IMMEDIATELY,
        START_IN_THREADS
    }

    private void basic(Mode mode) throws Exception
    {
        final int PARTICIPANT_QTY = 1;//0;

        List<LeaderLatch> latches = Lists.newArrayList();

        Timing timing = new Timing();
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        try
        {
            client.start();

            for ( int i = 0; i < PARTICIPANT_QTY; ++i )
            {
                LeaderLatch latch = new LeaderLatch(client, PATH_NAME);
                if ( mode == Mode.START_IMMEDIATELY )
                {
                    latch.start();
                }
                latches.add(latch);
            }
            if ( mode == Mode.START_IN_THREADS )
            {
                ExecutorService service = Executors.newFixedThreadPool(latches.size());
                for ( final LeaderLatch latch : latches )
                {
                    service.submit(new Callable<Object>()
                    {
                        @Override
                        public Object call() throws Exception
                        {
                            Thread.sleep((int)(100 * Math.random()));
                            latch.start();
                            return null;
                        }
                    });
                }
                service.shutdown();
            }

            while ( latches.size() > 0 )
            {
                List<LeaderLatch> leaders = waitForALeader(latches, timing);
                assertEquals(leaders.size(), 1); // there can only be one leader
                LeaderLatch theLeader = leaders.get(0);
                if ( mode == Mode.START_IMMEDIATELY )
                {
                    assertEquals(latches.indexOf(theLeader), 0); // assert ordering - leadership should advance in start order
                }
                theLeader.close();
                latches.remove(theLeader);
            }
        }
        finally
        {
            for ( LeaderLatch latch : latches )
            {
                CloseableUtils.closeQuietly(latch);
            }
            TestCleanState.closeAndTestClean(client);
        }
    }

    private List<LeaderLatch> waitForALeader(List<LeaderLatch> latches, Timing timing) throws InterruptedException
    {
        for ( int i = 0; i < MAX_LOOPS; ++i )
        {
            List<LeaderLatch> leaders = getLeaders(latches);
            if ( leaders.size() != 0 )
            {
                return leaders;
            }
            timing.sleepABit();
        }
        return Lists.newArrayList();
    }

    private List<LeaderLatch> getLeaders(Collection<LeaderLatch> latches)
    {
        List<LeaderLatch> leaders = Lists.newArrayList();
        for ( LeaderLatch latch : latches )
        {
            if ( latch.hasLeadership() )
            {
                leaders.add(latch);
            }
        }
        return leaders;
    }

    @Test
    public void testRelativePath()
    {
        assertThrows(IllegalArgumentException.class, ()->{
            Timing timing = new Timing();
            CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
            new LeaderLatch(client, "parent");
        });
    }
}
