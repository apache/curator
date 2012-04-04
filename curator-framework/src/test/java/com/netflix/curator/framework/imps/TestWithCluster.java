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

package com.netflix.curator.framework.imps;

import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.state.ConnectionState;
import com.netflix.curator.framework.state.ConnectionStateListener;
import com.netflix.curator.retry.RetryOneTime;
import com.netflix.curator.test.InstanceSpec;
import com.netflix.curator.test.TestingCluster;
import com.netflix.curator.test.Timing;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.concurrent.CountDownLatch;

public class TestWithCluster
{
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
            Closeables.closeQuietly(client);
            Closeables.closeQuietly(cluster);
        }
    }
}
