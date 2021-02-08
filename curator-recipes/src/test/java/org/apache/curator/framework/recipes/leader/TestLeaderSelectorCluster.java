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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.curator.test.compatibility.CuratorTestBase;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingCluster;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.ZKPaths;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
public class TestLeaderSelectorCluster extends CuratorTestBase
{
    @Test
    public void     testRestart() throws Exception
    {
        final Timing        timing = new Timing();

        CuratorFramework    client = null;
        TestingCluster      cluster = createAndStartCluster(3);
        try
        {
            client = CuratorFrameworkFactory.newClient(cluster.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
            client.start();

            final Semaphore             semaphore = new Semaphore(0);
            LeaderSelectorListener      listener = new LeaderSelectorListener()
            {
                @Override
                public void takeLeadership(CuratorFramework client) throws Exception
                {
                    List<String>        names = client.getChildren().forPath("/leader");
                    assertTrue(names.size() > 0);
                    semaphore.release();
                }

                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState)
                {
                }
            };
            LeaderSelector      selector = new LeaderSelector(client, "/leader", listener);
            selector.autoRequeue();
            selector.start();
            assertTrue(timing.acquireSemaphore(semaphore));

            InstanceSpec connectionInstance = cluster.findConnectionInstance(client.getZookeeperClient().getZooKeeper());
            cluster.killServer(connectionInstance);

            assertTrue(timing.multiple(4).acquireSemaphore(semaphore));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
            CloseableUtils.closeQuietly(cluster);
        }
    }

    @Test
    public void     testLostRestart() throws Exception
    {
        final Timing        timing = new Timing();

        CuratorFramework    client = null;
        TestingCluster      cluster = createAndStartCluster(3);
        try
        {
            client = CuratorFrameworkFactory.newClient(cluster.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
            client.start();
            client.sync().forPath("/");

            final AtomicReference<Exception>        error = new AtomicReference<Exception>(null);
            final AtomicReference<String>           lockNode = new AtomicReference<String>(null);
            final Semaphore                         semaphore = new Semaphore(0);
            final CountDownLatch                    lostLatch = new CountDownLatch(1);
            final CountDownLatch                    internalLostLatch = new CountDownLatch(1);
            LeaderSelectorListener                  listener = new LeaderSelectorListener()
            {
                @Override
                public void takeLeadership(CuratorFramework client) throws Exception
                {
                    try
                    {
                        List<String>        names = client.getChildren().forPath("/leader");
                        if ( names.size() != 1 )
                        {
                            semaphore.release();
                            Exception exception = new Exception("Names size isn't 1: " + names.size());
                            error.set(exception);
                            return;
                        }
                        lockNode.set(names.get(0));

                        semaphore.release();
                        if ( !timing.multiple(4).awaitLatch(internalLostLatch) )
                        {
                            error.set(new Exception("internalLostLatch await failed"));
                        }
                    }
                    finally
                    {
                        lostLatch.countDown();
                    }
                }

                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState)
                {
                    if ( newState == ConnectionState.LOST )
                    {
                        internalLostLatch.countDown();
                    }
                }
            };
            LeaderSelector      selector = new LeaderSelector(client, "/leader", listener);
            selector.start();
            assertTrue(timing.multiple(4).acquireSemaphore(semaphore));
            if ( error.get() != null )
            {
                throw new AssertionError(error.get());
            }

            Collection<InstanceSpec>    instances = cluster.getInstances();
            cluster.stop();

            assertTrue(timing.multiple(4).awaitLatch(lostLatch));
            timing.sleepABit();
            assertFalse(selector.hasLeadership());

            assertNotNull(lockNode.get());
            
            cluster = new TestingCluster(instances.toArray(new InstanceSpec[instances.size()]));
            cluster.start();

            try
            {
                client.delete().forPath(ZKPaths.makePath("/leader", lockNode.get()));   // simulate the lock deleting due to session expiration
            }
            catch ( Exception ignore )
            {
                // ignore
            }

            assertTrue(semaphore.availablePermits() == 0);
            assertFalse(selector.hasLeadership());

            selector.requeue();
            assertTrue(timing.multiple(4).acquireSemaphore(semaphore));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
            CloseableUtils.closeQuietly(cluster);
        }
    }

    @Override
    protected void createServer() throws Exception
    {
        // NOP
    }
}
