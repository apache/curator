/*
 *
 *  Copyright 2011 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.curator.framework.recipes.leader;

import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.recipes.BaseClassForTests;
import com.netflix.curator.framework.state.ConnectionState;
import com.netflix.curator.framework.state.ConnectionStateListener;
import com.netflix.curator.retry.RetryOneTime;
import com.netflix.curator.test.TestingServer;
import com.netflix.curator.test.Timing;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestLeaderLatch extends BaseClassForTests
{
    private static final String PATH_NAME = "/one/two/me";
    private static final int MAX_LOOPS = 5;

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

            final CountDownLatch        countDownLatch = new CountDownLatch(1);
            client.getConnectionStateListenable().addListener
            (
                new ConnectionStateListener()
                {
                    @Override
                    public void stateChanged(CuratorFramework client, ConnectionState newState)
                    {
                        if ( newState == ConnectionState.LOST )
                        {
                            countDownLatch.countDown();
                        }
                    }
                }
            );

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

            server = new TestingServer(server.getPort(), server.getTempDirectory());
            waitForALeader(latches, timing);    // should reconnect
            Assert.assertEquals(getLeaders(latches).size(), 1);
        }
        finally
        {
            for ( LeaderLatch latch : latches )
            {
                Closeables.closeQuietly(latch);
            }
            Closeables.closeQuietly(client);
        }
    }

    @Test
    public void testWaiting() throws Exception
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
                service.submit
                    (
                        new Callable<Void>()
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
                                }
                                finally
                                {
                                    thereIsALeader.set(false);
                                    latch.close();
                                }
                                return null;
                            }
                        }
                    );
            }

            for ( int i = 0; i < PARTICIPANT_QTY; ++i )
            {
                service.take().get();
            }
        }
        finally
        {
            executorService.shutdown();
            Closeables.closeQuietly(client);
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

    private enum Mode
    {
        START_IMMEDIATELY,
        START_IN_THREADS
    }

    private void basic(Mode mode) throws Exception
    {
        final int PARTICIPANT_QTY = 10;

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
                    service.submit
                    (
                        new Callable<Object>()
                        {
                            @Override
                            public Object call() throws Exception
                            {
                                Thread.sleep((int)(100 * Math.random()));
                                latch.start();
                                return null;
                            }
                        }
                    );
                }
                service.shutdown();
            }

            while ( latches.size() > 0 )
            {
                waitForALeader(latches, timing);

                List<LeaderLatch> leaders = getLeaders(latches);
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
                Closeables.closeQuietly(latch);
            }
            Closeables.closeQuietly(client);
        }
    }

    private void waitForALeader(List<LeaderLatch> latches, Timing timing) throws InterruptedException
    {
        for ( int i = 0; i < MAX_LOOPS; ++i )
        {
            if ( getLeaders(latches).size() != 0 )
            {
                break;
            }
            timing.sleepABit();
        }
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
}
