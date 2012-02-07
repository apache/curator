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

import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.state.ConnectionState;
import com.netflix.curator.retry.RetryOneTime;
import com.netflix.curator.test.TestingCluster;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class TestLeaderSelectorCluster
{
    @Test
    public void     testRestart() throws Exception
    {
        final int TIMEOUT_SECONDS = 5;

        CuratorFramework    client = null;
        TestingCluster      cluster = new TestingCluster(3);
        cluster.start();
        try
        {
            client = CuratorFrameworkFactory.newClient(cluster.getConnectString(), TIMEOUT_SECONDS * 1000, TIMEOUT_SECONDS * 1000, new RetryOneTime(1));
            client.start();
            
            final Semaphore             semaphore = new Semaphore(0);
            final CountDownLatch        reconnectedLatch = new CountDownLatch(1);
            LeaderSelectorListener      listener = new LeaderSelectorListener()
            {
                @Override
                public void takeLeadership(CuratorFramework client) throws Exception
                {
                    List<String>        names = client.getChildren().forPath("/leader");
                    Assert.assertTrue(names.size() > 0);
                    semaphore.release();
                }

                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState)
                {
                    if ( newState == ConnectionState.RECONNECTED )
                    {
                        reconnectedLatch.countDown();
                    }
                }
            };
            LeaderSelector      selector = new LeaderSelector(client, "/leader", listener);
            selector.start();
            Assert.assertTrue(semaphore.tryAcquire(10, TimeUnit.SECONDS));

            TestingCluster.InstanceSpec     connectionInstance = cluster.findConnectionInstance(client.getZookeeperClient().getZooKeeper());
            cluster.killServer(connectionInstance);

            Assert.assertTrue(reconnectedLatch.await(10, TimeUnit.SECONDS));
            selector.start();
            Assert.assertTrue(semaphore.tryAcquire(10, TimeUnit.SECONDS));
        }
        finally
        {
            Closeables.closeQuietly(client);
            Closeables.closeQuietly(cluster);
        }
    }

    @Test
    public void     testLostRestart() throws Exception
    {
        final int TIMEOUT_SECONDS = 5;

        CuratorFramework    client = null;
        TestingCluster      cluster = new TestingCluster(3);
        cluster.start();
        try
        {
            client = CuratorFrameworkFactory.newClient(cluster.getConnectString(), TIMEOUT_SECONDS * 1000, TIMEOUT_SECONDS * 1000, new RetryOneTime(1));
            client.start();

            final Semaphore         semaphore = new Semaphore(0);
            final CountDownLatch    lostLatch = new CountDownLatch(1);
            LeaderSelectorListener  listener = new LeaderSelectorListener()
            {
                @Override
                public void takeLeadership(CuratorFramework client) throws Exception
                {
                    List<String>        names = client.getChildren().forPath("/leader");
                    Assert.assertTrue(names.size() > 0);
                    semaphore.release();
                }

                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState)
                {
                    if ( newState == ConnectionState.LOST )
                    {
                        lostLatch.countDown();
                    }
                }
            };
            LeaderSelector      selector = new LeaderSelector(client, "/leader", listener);
            selector.start();
            Assert.assertTrue(semaphore.tryAcquire(TIMEOUT_SECONDS * 2, TimeUnit.SECONDS));

            Collection<TestingCluster.InstanceSpec>    instances = cluster.getInstances();
            cluster.close();
            cluster = null;

            Assert.assertTrue(lostLatch.await(TIMEOUT_SECONDS * 2, TimeUnit.SECONDS));
            
            cluster = new TestingCluster(instances.toArray(new TestingCluster.InstanceSpec[instances.size()]));
            cluster.start();

            selector.start();
            Assert.assertTrue(semaphore.tryAcquire(TIMEOUT_SECONDS * 2, TimeUnit.SECONDS));
        }
        finally
        {
            Closeables.closeQuietly(client);
            Closeables.closeQuietly(cluster);
        }
    }
}
