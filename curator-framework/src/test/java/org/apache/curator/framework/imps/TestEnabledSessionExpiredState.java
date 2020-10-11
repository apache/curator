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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.google.common.collect.Queues;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.compatibility.Timing2;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class TestEnabledSessionExpiredState extends BaseClassForTests
{
    private final Timing2 timing = new Timing2();

    private CuratorFramework client;
    private BlockingQueue<ConnectionState> states;

    @BeforeEach
    @Override
    public void setup() throws Exception
    {
        super.setup();

        client = CuratorFrameworkFactory.builder()
            .connectString(server.getConnectString())
            .connectionTimeoutMs(timing.connection())
            .sessionTimeoutMs(timing.session())
            .retryPolicy(new RetryOneTime(1))
            .build();
        client.start();

        states = Queues.newLinkedBlockingQueue();
        ConnectionStateListener listener = new ConnectionStateListener()
        {
            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState)
            {
                states.add(newState);
            }
        };
        client.getConnectionStateListenable().addListener(listener);
    }

    @AfterEach
    @Override
    public void teardown() throws Exception
    {
        try
        {
            CloseableUtils.closeQuietly(client);
        }
        finally
        {
            super.teardown();
        }
    }

    @Test
    public void testResetCausesLost() throws Exception
    {
        assertEquals(states.poll(timing.milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.CONNECTED);
        client.checkExists().forPath("/");  // establish initial connection

        client.getZookeeperClient().reset();
        assertEquals(states.poll(timing.milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.LOST);
        assertEquals(states.poll(timing.milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.RECONNECTED);
    }

    @Test
    public void testInjectedWatchedEvent() throws Exception
    {
        assertEquals(states.poll(timing.milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.CONNECTED);

        final CountDownLatch latch = new CountDownLatch(1);
        Watcher watcher = new Watcher()
        {
            @Override
            public void process(WatchedEvent event)
            {
                if ( event.getType() == Event.EventType.None )
                {
                    if ( event.getState() == Event.KeeperState.Expired )
                    {
                        latch.countDown();
                    }
                }
            }
        };
        client.checkExists().usingWatcher(watcher).forPath("/");
        server.stop();
        assertTrue(timing.forSessionSleep().awaitLatch(latch));
    }

    @Test
    public void testKillSession() throws Exception
    {
        assertEquals(states.poll(timing.milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.CONNECTED);

        client.getZookeeperClient().getZooKeeper().getTestable().injectSessionExpiration();

        assertEquals(states.poll(timing.forSessionSleep().milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.LOST);
        assertEquals(states.poll(timing.milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.RECONNECTED);
    }

    @Test
    public void testReconnectWithoutExpiration() throws Exception
    {
        assertEquals(states.poll(timing.milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.CONNECTED);
        server.stop();
        try
        {
            client.checkExists().forPath("/");  // any API call that will invoke the retry policy, etc.
        }
        catch ( KeeperException.ConnectionLossException ignore )
        {
        }
        assertEquals(states.poll(timing.milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.SUSPENDED);
        server.restart();
        client.checkExists().forPath("/");
        assertEquals(states.poll(timing.milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.RECONNECTED);
    }

    @Test
    public void testSessionExpirationFromTimeout() throws Exception
    {
        assertEquals(states.poll(timing.milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.CONNECTED);
        server.stop();
        assertEquals(states.poll(timing.milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.SUSPENDED);
        assertEquals(states.poll(timing.forSessionSleep().milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.LOST);
    }

    @Test
    public void testSessionExpirationFromTimeoutWithRestart() throws Exception
    {
        assertEquals(states.poll(timing.milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.CONNECTED);
        server.stop();
        timing.forSessionSleep().sleep();
        assertEquals(states.poll(timing.milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.SUSPENDED);
        assertEquals(states.poll(timing.forSessionSleep().milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.LOST);
        server.restart();
        client.checkExists().forPath("/");
        assertEquals(states.poll(timing.milliseconds(), TimeUnit.MILLISECONDS), ConnectionState.RECONNECTED);

        assertNull(states.poll(timing.multiple(.5).milliseconds(), TimeUnit.MILLISECONDS));  // there should be no other events
    }
}
