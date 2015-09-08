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

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.TestCleanState;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.framework.state.SessionConnectionStateErrorPolicy;
import org.apache.curator.framework.state.StandardConnectionStateErrorPolicy;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.TestingServer;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class TestLeaderLatch extends BaseClassForTests
{
    private static final String PATH_NAME = "/one/two/me";
    private static final int MAX_LOOPS = 5;

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
                Assert.assertEquals(states.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.CONNECTED.name());
                Assert.assertEquals(states.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS), "true");
                server.stop();
                if ( isSessionIteration )
                {
                    Assert.assertEquals(states.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.SUSPENDED.name());
                    server.restart();
                    Assert.assertEquals(states.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.RECONNECTED.name());
                    Assert.assertNull(states.poll(timing.milliseconds(), TimeUnit.MILLISECONDS));
                }
                else
                {
                    String s = states.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS);
                    Assert.assertTrue("false".equals(s) || ConnectionState.SUSPENDED.name().equals(s));
                    s = states.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS);
                    Assert.assertTrue("false".equals(s) || ConnectionState.SUSPENDED.name().equals(s));
                    server.restart();
                    Assert.assertEquals(states.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.RECONNECTED.name());
                    Assert.assertEquals(states.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS), "true");
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
        Timing timing = new Timing();
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
            Assert.assertEquals(states.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.CONNECTED.name());
            Assert.assertEquals(states.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS), "true");
            server.close();
            List<String> next = Lists.newArrayList();
            next.add(states.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS));
            next.add(states.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS));
            Assert.assertTrue(next.equals(Arrays.asList(ConnectionState.SUSPENDED.name(), "false")) || next.equals(Arrays.asList("false", ConnectionState.SUSPENDED.name())), next.toString());
            Assert.assertEquals(states.poll(timing.forSessionSleep().milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.LOST.name());
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
            Assert.assertEquals(states.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.CONNECTED.name());
            Assert.assertEquals(states.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS), "true");
            server.close();
            Assert.assertEquals(states.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.SUSPENDED.name());
            next = Lists.newArrayList();
            next.add(states.poll(timing.forSessionSleep().milliseconds(), TimeUnit.MILLISECONDS));
            next.add(states.poll(timing.forSessionSleep().milliseconds(), TimeUnit.MILLISECONDS));
            Assert.assertTrue(next.equals(Arrays.asList(ConnectionState.LOST.name(), "false")) || next.equals(Arrays.asList("false", ConnectionState.LOST.name())), next.toString());
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

            Assert.assertTrue(timing.awaitLatch(cancelStartTaskLatch));
            Assert.assertFalse(resetCalled.get());
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

            Assert.assertEquals(client.getChildren().forPath(PATH_NAME).size(), 1);
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

            Assert.assertEquals(client.getChildren().forPath(PATH_NAME).size(), 0);

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
            Assert.assertTrue(timing.awaitLatch(countDownLatch));

            timing.forWaiting().sleepABit();

            Assert.assertEquals(getLeaders(latches).size(), 0);

            server.restart();
            Assert.assertEquals(waitForALeader(latches, timing).size(), 1); // should reconnect
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
            }

            waitForALeader(latches, timing);

            //we need to close a Participant that doesn't be actual leader (first Participant) nor the last
            latches.get(PARTICIPANT_ID).close();

            //As the previous algorithm assumed that if the watched node is deleted gets the leadership
            //we need to ensure that the PARTICIPANT_ID-1 is not getting (wrongly) elected as leader.
            Assert.assertTrue(!latches.get(PARTICIPANT_ID - 1).hasLeadership());
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
                            Assert.assertTrue(latch.await(timing.forWaiting().seconds(), TimeUnit.SECONDS));
                            Assert.assertTrue(thereIsALeader.compareAndSet(false, true));
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

            Assert.assertEquals(masterCounter.get(), PARTICIPANT_QTY * 2);
            Assert.assertEquals(notLeaderCounter.get(), PARTICIPANT_QTY);
            for ( LeaderLatch latch : latches )
            {
                Assert.assertEquals(latch.getState(), LeaderLatch.State.CLOSED);
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

            Assert.assertEquals(masterCounter.get(), PARTICIPANT_QTY * 2);
            Assert.assertEquals(notLeaderCounter.get(), PARTICIPANT_QTY * 2 - SILENT_QTY);
            for ( LeaderLatch latch : latches )
            {
                Assert.assertEquals(latch.getState(), LeaderLatch.State.CLOSED);
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

            Assert.assertEquals(leader.getState(), LeaderLatch.State.CLOSED);
            Assert.assertEquals(notifiedLeader.getState(), LeaderLatch.State.CLOSED);

            Assert.assertEquals(masterCounter.get(), 1);
            Assert.assertEquals(notLeaderCounter.get(), 0);
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

            Assert.assertTrue(timing.awaitLatch(leaderCounter), "Not elected leader");

            Assert.assertEquals(leaderCount.get(), 1, "Elected too many times");
            Assert.assertEquals(notLeaderCount.get(), 0, "Unelected too many times");
        }
        catch ( Exception e )
        {
            Assert.fail("Unexpected exception", e);
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
                Assert.assertEquals(leaders.size(), 1); // there can only be one leader
                LeaderLatch theLeader = leaders.get(0);
                if ( mode == Mode.START_IMMEDIATELY )
                {
                    Assert.assertEquals(latches.indexOf(theLeader), 0); // assert ordering - leadership should advance in start order
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

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testRelativePath() throws Exception
    {
        Timing timing = new Timing();
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        new LeaderLatch(client, "parent");
    }
}
