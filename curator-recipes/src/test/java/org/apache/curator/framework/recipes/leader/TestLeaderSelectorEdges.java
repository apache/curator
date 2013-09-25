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

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.recipes.BaseClassForTests;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Test cases designed after CURATOR-45
 */
public class TestLeaderSelectorEdges extends BaseClassForTests
{
    private final Logger log = LoggerFactory.getLogger(getClass());

    @BeforeClass
    public static void setCNXFactory()
    {
        System.setProperty(ServerCnxnFactory.ZOOKEEPER_SERVER_CNXN_FACTORY, ChaosMonkeyCnxnFactory.class.getName());
    }

    @AfterClass
    public static void resetCNXFactory()
    {
        System.clearProperty(ServerCnxnFactory.ZOOKEEPER_SERVER_CNXN_FACTORY);
    }

    /**
     * Create a LeaderSelector but close the connection right after the "lock" znode
     * has been created.
     *
     * @throws Exception
     */
    @Test
    public void flappingTest() throws Exception
    {
        final CuratorFramework client =
            CuratorFrameworkFactory.builder()
                .connectString(server.getConnectString())
                .retryPolicy(new RetryNTimes(1, 500))
                .sessionTimeoutMs(30000)
                .build();

        final TestLeaderSelectorListener listener = new TestLeaderSelectorListener();
        LeaderSelector leaderSelector1 =
            new LeaderSelector(client, ChaosMonkeyCnxnFactory.CHAOS_ZNODE, listener);
        LeaderSelector leaderSelector2 = null;

        client.start();
        try
        {
            client.create().forPath(ChaosMonkeyCnxnFactory.CHAOS_ZNODE);
            leaderSelector1.start();
            // At this point the ChaosMonkeyZookeeperServer must close the connection
            // right after the lock znode is created.
            Assert.assertTrue(listener.reconnected.await(10, TimeUnit.SECONDS), "Connection has not been lost");
            // Check that leader ship has failed
            Assert.assertEquals(listener.takeLeadership.getCount(), 1);
            // Wait FailedDelete
            Thread.sleep(ChaosMonkeyCnxnFactory.LOCKOUT_DURATION_MS * 2);
            // Check that there is no znode
            final int children = client.getChildren().forPath(ChaosMonkeyCnxnFactory.CHAOS_ZNODE).size();
            Assert.assertEquals(children, 0,
                "Still " + children + " znodes under " + ChaosMonkeyCnxnFactory.CHAOS_ZNODE + " lock");
            // Check that a new LeaderSelector can be started
            leaderSelector2 = new LeaderSelector(client, ChaosMonkeyCnxnFactory.CHAOS_ZNODE,
                listener);
            leaderSelector2.start();
            Assert.assertTrue(listener.takeLeadership.await(1, TimeUnit.SECONDS));
        }
        finally
        {
            try
            {
                leaderSelector1.close();
            }
            catch ( IllegalStateException e )
            {
                Assert.fail(e.getMessage());
            }
            try
            {
                if ( leaderSelector2 != null )
                {
                    leaderSelector2.close();
                }
            }
            catch ( IllegalStateException e )
            {
                Assert.fail(e.getMessage());
            }
            client.close();
        }
    }

    private class TestLeaderSelectorListener implements LeaderSelectorListener
    {
        final CountDownLatch takeLeadership = new CountDownLatch(1);
        final CountDownLatch reconnected = new CountDownLatch(1);

        @Override
        public void takeLeadership(CuratorFramework client) throws Exception
        {
            log.info("-->takeLeadership({})", client.toString());
            takeLeadership.countDown();
            log.info("<--takeLeadership({})", client.toString());
        }

        @Override
        public void stateChanged(CuratorFramework client, ConnectionState newState)
        {
            if ( newState == ConnectionState.RECONNECTED )
            {
                reconnected.countDown();
            }
        }

    }

    /**
     * Create a protected node in background with a retry policy
     */
    @Test
    public void createProtectedNodeInBackgroundTest() throws Exception
    {
        final CuratorFramework client =
            CuratorFrameworkFactory.builder()
                .connectString(server.getConnectString())
                .retryPolicy(new RetryNTimes(2, 1))
                .connectionTimeoutMs(100)
                .sessionTimeoutMs(60000)
                .build();
        final CountDownLatch latch = new CountDownLatch(1);
        client.start();
        try
        {
            client.create().forPath(ChaosMonkeyCnxnFactory.CHAOS_ZNODE);
            client.create()
                .withProtection()
                .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                .inBackground(
                    new BackgroundCallback()
                    {
                        public void processResult(CuratorFramework client, CuratorEvent event)
                            throws Exception
                        {
                            log.info("Receive event {}", event.toString());
                            if ( event.getResultCode() == KeeperException.Code.CONNECTIONLOSS.intValue() )
                            {
                                latch.countDown();
                            }
                        }
                    }
                             )
                .forPath(ChaosMonkeyCnxnFactory.CHAOS_ZNODE_PREFIX + "foo-");

            Assert.assertTrue(latch.await(30, TimeUnit.SECONDS), "Callback has not been called");
            // Wait for the znode to be deleted
            Thread.sleep(ChaosMonkeyCnxnFactory.LOCKOUT_DURATION_MS * 2);
            // Check that there is no znode
            final int children = client.getChildren().forPath(ChaosMonkeyCnxnFactory.CHAOS_ZNODE).size();
            Assert.assertEquals(children, 0,
                "Still " + children + " znodes under " + ChaosMonkeyCnxnFactory.CHAOS_ZNODE + " lock");
        }
        finally
        {
            client.close();
        }

    }

    /**
     * Same test as above but without a retry policy
     */
    @Test
    public void createProtectedNodeInBackgroundTestNoRetry() throws Exception
    {
        final CuratorFramework client =
            CuratorFrameworkFactory.builder()
                .connectString(server.getConnectString())
                .retryPolicy(new RetryNTimes(0, 0))
                .connectionTimeoutMs(100)
                .sessionTimeoutMs(60000)
                .build();
        final CountDownLatch latch = new CountDownLatch(1);
        client.start();
        try
        {
            client.create().forPath(ChaosMonkeyCnxnFactory.CHAOS_ZNODE);
            client.create()
                .withProtection()
                .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                .inBackground(
                    new BackgroundCallback()
                    {
                        public void processResult(CuratorFramework client, CuratorEvent event)
                            throws Exception
                        {
                            log.info("Receive event {}", event.toString());
                            if ( event.getResultCode() == KeeperException.Code.CONNECTIONLOSS.intValue() )
                            {
                                latch.countDown();
                            }
                        }
                    }
                             )
                .forPath(ChaosMonkeyCnxnFactory.CHAOS_ZNODE_PREFIX + "foo-");

            Assert.assertTrue(latch.await(30, TimeUnit.SECONDS), "Callback has not been called");
            // Wait for the znode to be deleted
            Thread.sleep(ChaosMonkeyCnxnFactory.LOCKOUT_DURATION_MS * 2);
            // Check that there is no znode
            final int children = client.getChildren().forPath(ChaosMonkeyCnxnFactory.CHAOS_ZNODE).size();
            Assert.assertEquals(children, 0,
                "Still " + children + " znodes under " + ChaosMonkeyCnxnFactory.CHAOS_ZNODE + " lock");
        }
        finally
        {
            client.close();
        }

    }
}
