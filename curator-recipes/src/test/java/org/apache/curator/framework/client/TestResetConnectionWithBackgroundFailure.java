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

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.TestingServer;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestResetConnectionWithBackgroundFailure extends BaseClassForTests
{
    private final Logger log = LoggerFactory.getLogger(getClass());

    @BeforeMethod
    @Override
    public void setup() throws Exception
    {
        super.setup();
    }

    @Test
    public void testConnectionStateListener() throws Exception
    {
        server.stop();

        final StringBuilder listenerSequence = new StringBuilder();
        LeaderSelector selector = null;
        Timing timing = new Timing();
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(100));
        try
        {
            client.start();
            timing.sleepABit();

            LeaderSelectorListener listenerLeader = new LeaderSelectorListenerAdapter()
            {
                @Override
                public void takeLeadership(CuratorFramework client) throws Exception
                {
                    Thread.currentThread().join();
                }
            };
            selector = new LeaderSelector(client, "/leader", listenerLeader);
            selector.autoRequeue();
            selector.start();

            ConnectionStateListener listener1 = new ConnectionStateListener()
            {
                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState)
                {
                    listenerSequence.append("-").append(newState);
                }
            };

            client.getConnectionStateListenable().addListener(listener1);
            log.debug("Starting ZK server");
            server.restart();
            timing.forWaiting().sleepABit();

            log.debug("Stopping ZK server");
            server.stop();
            timing.forWaiting().sleepABit();

            log.debug("Starting ZK server");
            server.restart();
            timing.forWaiting().sleepABit();

            log.debug("Stopping ZK server");
            server.close();
            timing.forWaiting().sleepABit();

            Assert.assertEquals(listenerSequence.toString(), "-CONNECTED-SUSPENDED-LOST-RECONNECTED-SUSPENDED-LOST");
        }
        finally
        {
            CloseableUtils.closeQuietly(selector);
            CloseableUtils.closeQuietly(client);
        }
    }

}