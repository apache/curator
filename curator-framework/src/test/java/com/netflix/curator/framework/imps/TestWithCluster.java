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
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.netflix.curator.retry.RetryOneTime;
import com.netflix.curator.test.InstanceSpec;
import com.netflix.curator.test.TestingCluster;
import com.netflix.curator.test.Timing;
import org.apache.zookeeper.CreateMode;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;

public class TestWithCluster
{
    @Test
    public void     testReadOnly() throws Exception
    {
        System.setProperty("readonlymode.enabled", "true");
        try
        {
            Timing              timing = new Timing();

            CuratorFramework    client = null;
            TestingCluster      cluster = new TestingCluster(2);
            try
            {
                cluster.start();

                client = CuratorFrameworkFactory.builder()
                    .connectString(cluster.getConnectString())
                    .canBeReadOnly(true)
                    .connectionTimeoutMs(timing.connection())
                    .sessionTimeoutMs(timing.session())
                    .retryPolicy(new ExponentialBackoffRetry(100, 3))
                    .build();
                client.start();

                client.create().forPath("/test");

                final CountDownLatch        readOnlyLatch = new CountDownLatch(1);
                final CountDownLatch        reconnectedLatch = new CountDownLatch(1);
                ConnectionStateListener     listener = new ConnectionStateListener()
                {
                    @Override
                    public void stateChanged(CuratorFramework client, ConnectionState newState)
                    {
                        if ( newState == ConnectionState.READ_ONLY )
                        {
                            readOnlyLatch.countDown();
                        }
                        else if ( newState == ConnectionState.RECONNECTED )
                        {
                            reconnectedLatch.countDown();
                        }
                    }
                };
                client.getConnectionStateListenable().addListener(listener);

                InstanceSpec                ourInstance = cluster.findConnectionInstance(client.getZookeeperClient().getZooKeeper());
                Iterator<InstanceSpec>      iterator = cluster.getInstances().iterator();
                InstanceSpec                killInstance = iterator.next();
                if ( killInstance.equals(ourInstance) )
                {
                    killInstance = iterator.next(); // kill the instance we're not connected to
                }
                cluster.killServer(killInstance);

                Assert.assertEquals(reconnectedLatch.getCount(), 1);
                Assert.assertTrue(timing.awaitLatch(readOnlyLatch));

                Assert.assertEquals(reconnectedLatch.getCount(), 1);
                cluster.restartServer(killInstance);
                Assert.assertTrue(timing.awaitLatch(reconnectedLatch));
            }
            finally
            {
                Closeables.closeQuietly(client);
                Closeables.closeQuietly(cluster);
            }
        }
        finally
        {
            System.clearProperty("readonlymode.enabled");
        }
    }

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

            client.create().withMode(CreateMode.EPHEMERAL).forPath("/temp", "value".getBytes());
            Assert.assertNotNull(client.checkExists().forPath("/temp"));

            for ( InstanceSpec spec : cluster.getInstances() )
            {
                cluster.killServer(spec);
                timing.forWaiting().sleepABit();
                cluster.restartServer(spec);
                timing.sleepABit();
            }

            timing.sleepABit();
            Assert.assertNotNull(client.checkExists().forPath("/temp"));
        }
        finally
        {
            Closeables.closeQuietly(client);
            Closeables.closeQuietly(cluster);
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
            Closeables.closeQuietly(client);
            Closeables.closeQuietly(cluster);
        }
    }
}
