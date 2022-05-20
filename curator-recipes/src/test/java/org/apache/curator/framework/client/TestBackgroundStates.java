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

package org.apache.curator.framework.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.google.common.collect.Queues;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.TestingServer;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.junit.jupiter.api.Test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

// NOTE: these tests are in Framework as they use the PersistentEphemeralNode recipe

public class TestBackgroundStates extends BaseClassForTests
{
    @Test
    public void testListenersReconnectedIsOK() throws Exception
    {
        server.close();

        Timing timing = new Timing();
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        PersistentEphemeralNode node = null;
        try
        {
            client.start();
            node = new PersistentEphemeralNode(client, PersistentEphemeralNode.Mode.EPHEMERAL, "/abc/node", "hello".getBytes());
            node.start();

            final CountDownLatch connectedLatch = new CountDownLatch(1);
            final CountDownLatch reconnectedLatch = new CountDownLatch(1);
            final AtomicReference<ConnectionState> lastState = new AtomicReference<ConnectionState>();
            ConnectionStateListener listener = new ConnectionStateListener()
            {
                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState)
                {
                    lastState.set(newState);
                    if ( newState == ConnectionState.CONNECTED )
                    {
                        connectedLatch.countDown();
                    }
                    if ( newState == ConnectionState.RECONNECTED )
                    {
                        reconnectedLatch.countDown();
                    }
                }
            };
            client.getConnectionStateListenable().addListener(listener);
            timing.sleepABit();
            server = new TestingServer(server.getPort());
            assertTrue(timing.awaitLatch(connectedLatch));
            timing.sleepABit();
            assertTrue(node.waitForInitialCreate(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS));
            server.restart();
            timing.sleepABit();
            assertTrue(timing.awaitLatch(reconnectedLatch));
            timing.sleepABit();
            assertEquals(lastState.get(), ConnectionState.RECONNECTED);
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
            CloseableUtils.closeQuietly(node);
        }
    }

    @Test
    public void testConnectionStateListener() throws Exception
    {
        server.close();

        Timing timing = new Timing();
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(timing.milliseconds()));
        try
        {
            client.start();

            final BlockingQueue<ConnectionState> stateVector = Queues.newLinkedBlockingQueue(1);
            ConnectionStateListener listener = new ConnectionStateListener()
            {
                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState)
                {
                    stateVector.offer(newState);
                }
            };

            Timing waitingTiming = timing.forWaiting();

            client.getConnectionStateListenable().addListener(listener);
            server = new TestingServer(server.getPort());
            assertEquals(stateVector.poll(waitingTiming.milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.CONNECTED);
            server.stop();
            assertEquals(stateVector.poll(waitingTiming.milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.SUSPENDED);
            assertEquals(stateVector.poll(waitingTiming.milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.LOST);
            server.restart();
            assertEquals(stateVector.poll(waitingTiming.milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.RECONNECTED);
            server.close();
            assertEquals(stateVector.poll(waitingTiming.milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.SUSPENDED);
            assertEquals(stateVector.poll(waitingTiming.milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.LOST);
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

}