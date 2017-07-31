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

import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingCluster;
import org.apache.curator.test.Timing;
import org.apache.zookeeper.CreateMode;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.concurrent.CountDownLatch;

public class TestWithCluster
{
    @Test
    public void     testSessionSurvives() throws Exception
    {
        Timing              timing = new Timing();

        CuratorFramework    client = null;
        TestingCluster      cluster = new TestingCluster(3);
        cluster.start();
        try
        {
            client = CuratorFrameworkFactory.newClient(cluster.getConnectString(), timing.session(), timing.connection(), new ExponentialBackoffRetry(100, 3));
            client.start();

            final CountDownLatch reconnectedLatch = new CountDownLatch(1);
            ConnectionStateListener listener = new ConnectionStateListener()
            {
                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState)
                {
                    if ( newState == ConnectionState.RECONNECTED )
                    {
                        reconnectedLatch.countDown();;
                    }
                }
            };
            client.getConnectionStateListenable().addListener(listener);

            client.create().withMode(CreateMode.EPHEMERAL).forPath("/temp", "value".getBytes());
            Assert.assertNotNull(client.checkExists().forPath("/temp"));

            for ( InstanceSpec spec : cluster.getInstances() )
            {
                cluster.killServer(spec);
                timing.forWaiting().sleepABit();
                cluster.restartServer(spec);
                timing.sleepABit();
            }

            Assert.assertTrue(timing.awaitLatch(reconnectedLatch));
            Assert.assertNotNull(client.checkExists().forPath("/temp"));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
            CloseableUtils.closeQuietly(cluster);
        }
    }

    @Test
    public void     testSplitBrain() throws Exception
    {
        Timing              timing = new Timing();
        
        CuratorFramework    client = null;
        TestingCluster cluster = new TestingCluster(3);
        cluster.start();
        try
        {
            // make sure all instances are up
            for ( InstanceSpec instanceSpec : cluster.getInstances() )
            {
                client = CuratorFrameworkFactory.newClient(instanceSpec.getConnectString(), new RetryOneTime(1));
                client.start();
                client.checkExists().forPath("/");
                client.close();
                client = null;
            }

            client = CuratorFrameworkFactory.newClient(cluster.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
            client.start();

            final CountDownLatch        latch = new CountDownLatch(2);
            client.getConnectionStateListenable().addListener
            (
                new ConnectionStateListener()
                {
                    @Override
                    public void stateChanged(CuratorFramework client, ConnectionState newState)
                    {
                        if ( (newState == ConnectionState.SUSPENDED) || (newState == ConnectionState.LOST) )
                        {
                            latch.countDown();
                        }
                    }
                }
            );

            client.checkExists().forPath("/");

            for ( InstanceSpec instanceSpec : cluster.getInstances() )
            {
                if ( !instanceSpec.equals(cluster.findConnectionInstance(client.getZookeeperClient().getZooKeeper())) )
                {
                    Assert.assertTrue(cluster.killServer(instanceSpec));
                }
            }

            Assert.assertTrue(timing.awaitLatch(latch));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
            CloseableUtils.closeQuietly(cluster);
        }
    }
}
