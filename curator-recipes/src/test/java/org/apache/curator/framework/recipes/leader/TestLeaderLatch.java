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
import com.google.common.io.Closeables;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.BaseClassForTests;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.curator.test.Timing;
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
            Closeables.closeQuietly(latch);
            Closeables.closeQuietly(client);
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
            Assert.assertEquals(waitForALeader(latches, timing).size(), 1); // should reconnect
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
             Assert.assertTrue(!latches.get(PARTICIPANT_ID-1).hasLeadership());
	     }
	     finally
	     {
	    	 //removes the already closed participant
	    	 latches.remove(PARTICIPANT_ID);
	    	 
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
                Closeables.closeQuietly(latch);
            }
            Closeables.closeQuietly(client);
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
}
