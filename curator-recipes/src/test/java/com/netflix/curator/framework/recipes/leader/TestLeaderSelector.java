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
import com.netflix.curator.framework.recipes.KillSessionAndWait;
import com.netflix.curator.framework.state.ConnectionState;
import com.netflix.curator.retry.RetryOneTime;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.internal.annotations.Sets;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TestLeaderSelector extends BaseClassForTests
{
    private static final String     PATH_NAME = "/one/two/me";

    @Test
    public void     testServerDying() throws Exception
    {
        LeaderSelector      selector = null;
        CuratorFramework    client = CuratorFrameworkFactory.builder().connectString(server.getConnectString()).retryPolicy(new RetryOneTime(1)).sessionTimeoutMs(1000).build();
        client.start();
        try
        {
            final Semaphore             semaphore = new Semaphore(0);
            LeaderSelectorListener      listener = new LeaderSelectorListener()
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

            Assert.assertTrue(semaphore.tryAcquire(10, TimeUnit.SECONDS));

            server.close();

            Assert.assertTrue(semaphore.tryAcquire(10, TimeUnit.SECONDS));
        }
        finally
        {
            Closeables.closeQuietly(selector);
            Closeables.closeQuietly(client);
        }
    }

    @Test
    public void     testKillSession() throws Exception
    {
        final int TIMEOUT_SECONDS = 5000;

        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), TIMEOUT_SECONDS * 1000, TIMEOUT_SECONDS * 1000, new RetryOneTime(1));
        client.start();
        try
        {
            final Semaphore         semaphore = new Semaphore(0);
            final CountDownLatch    interruptedLatch = new CountDownLatch(1);
            final AtomicInteger     leaderCount = new AtomicInteger(0);
            LeaderSelectorListener  listener = new LeaderSelectorListener()
            {
                private volatile Thread     ourThread;

                @Override
                public void takeLeadership(CuratorFramework client) throws Exception
                {
                    leaderCount.incrementAndGet();
                    try
                    {
                        ourThread = Thread.currentThread();
                        semaphore.release();
                        try
                        {
                            Thread.sleep(1000000);
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

                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState)
                {
                    if ( (newState == ConnectionState.LOST) && (ourThread != null) )
                    {
                        ourThread.interrupt();
                    }
                }
            };
            LeaderSelector leaderSelector1 = new LeaderSelector(client, PATH_NAME, listener);
            LeaderSelector leaderSelector2 = new LeaderSelector(client, PATH_NAME, listener);

            leaderSelector1.start();
            leaderSelector2.start();

            Assert.assertTrue(semaphore.tryAcquire(1, TIMEOUT_SECONDS * 2, TimeUnit.SECONDS));

            KillSessionAndWait.kill(client, server.getConnectString());

            Assert.assertTrue(interruptedLatch.await(TIMEOUT_SECONDS * 2, TimeUnit.SECONDS));
            Assert.assertTrue(semaphore.tryAcquire(1, TIMEOUT_SECONDS * 2, TimeUnit.SECONDS));
            Assert.assertEquals(leaderCount.get(), 1);

            leaderSelector1.close();
            leaderSelector2.close();
        }
        finally
        {
            client.close();
        }
    }

    @Test
    public void     testClosing() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            final CountDownLatch        latch = new CountDownLatch(1);
            LeaderSelector leaderSelector1 = new LeaderSelector(client, PATH_NAME, new LeaderSelectorListener()
            {
                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState)
                {
                }

                @Override
                public void takeLeadership(CuratorFramework client) throws Exception
                {
                    latch.await();
                }
            });

            LeaderSelector      leaderSelector2 = new LeaderSelector(client, PATH_NAME, new LeaderSelectorListener()
            {
                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState)
                {
                }

                @Override
                public void takeLeadership(CuratorFramework client) throws Exception
                {
                    latch.await();
                }
            });

            leaderSelector1.start();
            leaderSelector2.start();

            while ( !leaderSelector1.hasLeadership() && !leaderSelector2.hasLeadership() )
            {
                Thread.sleep(1000);
            }

            Assert.assertNotSame(leaderSelector1.hasLeadership(), leaderSelector2.hasLeadership());

            LeaderSelector      positiveLeader;
            LeaderSelector      negativeLeader;
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
    public void     testRotatingLeadership() throws Exception
    {
        final int               LEADER_QTY = 5;
        final int               REPEAT_QTY = 3;

        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            final BlockingQueue<Integer>    leaderList = new LinkedBlockingQueue<Integer>();
            List<LeaderSelector>            selectors = Lists.newArrayList();
            for ( int i = 0; i < LEADER_QTY; ++i )
            {
                final int           ourIndex = i;
                LeaderSelector      leaderSelector = new LeaderSelector(client, PATH_NAME, new LeaderSelectorListener()
                {
                    @Override
                    public void takeLeadership(CuratorFramework client) throws Exception
                    {
                        Thread.sleep(500);
                        leaderList.add(ourIndex);
                    }

                    @Override
                    public void stateChanged(CuratorFramework client, ConnectionState newState)
                    {
                    }
                });
                selectors.add(leaderSelector);
            }

            List<Integer>                 localLeaderList = Lists.newArrayList();
            for ( int i = 1; i <= REPEAT_QTY; ++i )
            {
                for ( LeaderSelector leaderSelector : selectors )
                {
                    leaderSelector.start();
                }

                while ( localLeaderList.size() != (i * selectors.size()) )
                {
                    Integer polledIndex = leaderList.poll(10, TimeUnit.SECONDS);
                    Assert.assertNotNull(polledIndex);
                    localLeaderList.add(polledIndex);
                }
                Thread.sleep(500);
            }

            for ( LeaderSelector leaderSelector : selectors )
            {
                leaderSelector.close();
            }
            System.out.println(localLeaderList);

            for ( int i = 0; i < REPEAT_QTY; ++i )
            {
                Set<Integer>        uniques = Sets.newHashSet();
                for ( int j = 0; j < selectors.size(); ++j )
                {
                    Assert.assertTrue(localLeaderList.size() > 0);

                    int     thisIndex = localLeaderList.remove(0);
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
