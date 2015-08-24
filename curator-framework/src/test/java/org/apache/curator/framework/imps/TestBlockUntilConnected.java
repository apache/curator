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

package org.apache.curator.framework.imps;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.TestingServer;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class TestBlockUntilConnected extends BaseClassForTests
{
    /**
     * Test the case where we're already connected
     */
    @Test
    public void testBlockUntilConnectedCurrentlyConnected() throws Exception
    {
        Timing timing = new Timing();
        CuratorFramework client = CuratorFrameworkFactory.builder().
            connectString(server.getConnectString()).
            retryPolicy(new RetryOneTime(1)).
            build();

        try
        {
            final CountDownLatch connectedLatch = new CountDownLatch(1);
            client.getConnectionStateListenable().addListener(new ConnectionStateListener()
            {
                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState)
                {
                    if ( newState.isConnected() )
                    {
                        connectedLatch.countDown();
                    }
                }
            });

            client.start();

            Assert.assertTrue(timing.awaitLatch(connectedLatch), "Timed out awaiting latch");
            Assert.assertTrue(client.blockUntilConnected(1, TimeUnit.SECONDS), "Not connected");
        }
        catch ( InterruptedException e )
        {
            Assert.fail("Unexpected interruption");
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    /**
     * Test the case where we are not currently connected and never have been
     */
    @Test
    public void testBlockUntilConnectedCurrentlyNeverConnected()
    {
        CuratorFramework client = CuratorFrameworkFactory.builder().
            connectString(server.getConnectString()).
            retryPolicy(new RetryOneTime(1)).
            build();

        try
        {
            client.start();
            Assert.assertTrue(client.blockUntilConnected(5, TimeUnit.SECONDS), "Not connected");
        }
        catch ( InterruptedException e )
        {
            Assert.fail("Unexpected interruption");
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    /**
     * Test the case where we are not currently connected, but have been previously
     */
    @Test
    public void testBlockUntilConnectedCurrentlyAwaitingReconnect()
    {
        Timing timing = new Timing();
        CuratorFramework client = CuratorFrameworkFactory.builder().
            connectString(server.getConnectString()).
            sessionTimeoutMs(timing.session()).
            retryPolicy(new RetryOneTime(1)).
            build();

        final CountDownLatch lostLatch = new CountDownLatch(1);
        client.getConnectionStateListenable().addListener(new ConnectionStateListener()
        {

            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState)
            {
                if ( newState == ConnectionState.LOST )
                {
                    lostLatch.countDown();
                }
            }
        });

        try
        {
            client.start();

            //Block until we're connected
            Assert.assertTrue(client.blockUntilConnected(5, TimeUnit.SECONDS), "Failed to connect");

            //Kill the server
            CloseableUtils.closeQuietly(server);

            //Wait until we hit the lost state
            Assert.assertTrue(timing.awaitLatch(lostLatch), "Failed to reach LOST state");

            server = new TestingServer(server.getPort(), server.getTempDirectory());

            Assert.assertTrue(client.blockUntilConnected(5, TimeUnit.SECONDS), "Not connected");
        }
        catch ( Exception e )
        {
            Assert.fail("Unexpected exception " + e);
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    /**
     * Test the case where we are not currently connected and time out before a
     * connection becomes available.
     */
    @Test
    public void testBlockUntilConnectedConnectTimeout()
    {
        //Kill the server
        CloseableUtils.closeQuietly(server);

        CuratorFramework client = CuratorFrameworkFactory.builder().
            connectString(server.getConnectString()).
            retryPolicy(new RetryOneTime(1)).
            build();

        try
        {
            client.start();
            Assert.assertFalse(client.blockUntilConnected(5, TimeUnit.SECONDS), "Connected");
        }
        catch ( InterruptedException e )
        {
            Assert.fail("Unexpected interruption");
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    /**
     * Test the case where we are not currently connected and the thread gets interrupted
     * prior to a connection becoming available
     */
    @Test
    public void testBlockUntilConnectedInterrupt()
    {
        //Kill the server
        CloseableUtils.closeQuietly(server);

        final CuratorFramework client = CuratorFrameworkFactory.builder().
            connectString(server.getConnectString()).
            retryPolicy(new RetryOneTime(1)).
            build();

        try
        {
            client.start();

            final Thread threadToInterrupt = Thread.currentThread();

            Timer timer = new Timer();
            timer.schedule(new TimerTask()
            {

                @Override
                public void run()
                {
                    threadToInterrupt.interrupt();
                }
            }, 3000);

            client.blockUntilConnected(5, TimeUnit.SECONDS);
            Assert.fail("Expected interruption did not occur");
        }
        catch ( InterruptedException e )
        {
            //This is expected
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    /**
     * Test that we are actually connected every time that we block until connection is established in a tight loop.
     */
    @Test
    public void testBlockUntilConnectedTightLoop() throws InterruptedException
    {
        CuratorFramework client;
        for(int i = 0 ; i < 50 ; i++)
        {
            client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(100));
            try
            {
                client.start();
                client.blockUntilConnected();

                Assert.assertTrue(client.getZookeeperClient().isConnected(), "Not connected after blocking for connection #" + i);
            }
            finally
            {
                client.close();
            }
        }
    }
}
