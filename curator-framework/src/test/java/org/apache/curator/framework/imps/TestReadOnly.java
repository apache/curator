/*
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

import com.google.common.io.Closeables;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingCluster;
import org.apache.curator.test.Timing;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;

public class TestReadOnly
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
}
