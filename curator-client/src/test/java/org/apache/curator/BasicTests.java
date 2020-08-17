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
package org.apache.curator;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import io.github.artsok.RepeatedIfExceptionsTest;
import org.apache.curator.ensemble.fixed.FixedEnsembleProvider;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.ZookeeperFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.mockito.Mockito;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class BasicTests extends BaseClassForTests
{
    @RepeatedIfExceptionsTest(repeats = BaseClassForTests.REPEATS)
    public void     testFactory() throws Exception
    {
        final ZooKeeper         mockZookeeper = Mockito.mock(ZooKeeper.class);
        ZookeeperFactory        zookeeperFactory = new ZookeeperFactory()
        {
            @Override
            public ZooKeeper newZooKeeper(String connectString, int sessionTimeout, Watcher watcher, boolean canBeReadOnly) throws Exception
            {
                return mockZookeeper;
            }
        };
        CuratorZookeeperClient  client = new CuratorZookeeperClient(zookeeperFactory, new FixedEnsembleProvider(server.getConnectString()), 10000, 10000, null, new RetryOneTime(1), false);
        client.start();
        assertEquals(client.getZooKeeper(), mockZookeeper);
    }

    @RepeatedIfExceptionsTest(repeats = BaseClassForTests.REPEATS)
    public void     testExpiredSession() throws Exception
    {
        // see http://wiki.apache.org/hadoop/ZooKeeper/FAQ#A4

        final Timing                  timing = new Timing();

        final CountDownLatch    latch = new CountDownLatch(1);
        Watcher                 watcher = new Watcher()
        {
            @Override
            public void process(WatchedEvent event)
            {
                if ( event.getState() == Event.KeeperState.Expired )
                {
                    latch.countDown();
                }
            }
        };

        final CuratorZookeeperClient client = new CuratorZookeeperClient(server.getConnectString(), timing.session(), timing.connection(), watcher, new RetryOneTime(2));
        client.start();
        try
        {
            final AtomicBoolean     firstTime = new AtomicBoolean(true);
            RetryLoop.callWithRetry
            (
                client,
                new Callable<Object>()
                {
                    @Override
                    public Object call() throws Exception
                    {
                        if ( firstTime.compareAndSet(true, false) )
                        {
                            try
                            {
                                client.getZooKeeper().create("/foo", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                            }
                            catch ( KeeperException.NodeExistsException ignore )
                            {
                                // ignore
                            }

                            client.getZooKeeper().getTestable().injectSessionExpiration();

                            assertTrue(timing.awaitLatch(latch));
                        }
                        ZooKeeper zooKeeper = client.getZooKeeper();
                        client.blockUntilConnectedOrTimedOut();
                        assertNotNull(zooKeeper.exists("/foo", false));
                        return null;
                    }
                }
            );
        }
        finally
        {
            client.close();
        }
    }

    @RepeatedIfExceptionsTest(repeats = BaseClassForTests.REPEATS)
    public void     testReconnect() throws Exception
    {
        CuratorZookeeperClient client = new CuratorZookeeperClient(server.getConnectString(), 10000, 10000, null, new RetryOneTime(1));
        client.start();
        try
        {
            client.blockUntilConnectedOrTimedOut();

            byte[]      writtenData = {1, 2, 3};
            client.getZooKeeper().create("/test", writtenData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            Thread.sleep(1000);
            server.stop();
            Thread.sleep(1000);

            server.restart();
            assertTrue(client.blockUntilConnectedOrTimedOut());
            byte[]      readData = client.getZooKeeper().getData("/test", false, null);
            assertArrayEquals(readData, writtenData);
        }
        finally
        {
            client.close();
        }
    }

    @RepeatedIfExceptionsTest(repeats = BaseClassForTests.REPEATS)
    public void     testSimple() throws Exception
    {
        CuratorZookeeperClient client = new CuratorZookeeperClient(server.getConnectString(), 10000, 10000, null, new RetryOneTime(1));
        client.start();
        try
        {
            client.blockUntilConnectedOrTimedOut();
            String              path = client.getZooKeeper().create("/test", new byte[]{1,2,3}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            assertEquals(path, "/test");
        }
        finally
        {
            client.close();
        }
    }

    @RepeatedIfExceptionsTest(repeats = BaseClassForTests.REPEATS)
    public void     testBackgroundConnect() throws Exception
    {
        final int CONNECTION_TIMEOUT_MS = 4000;

        CuratorZookeeperClient client = new CuratorZookeeperClient(server.getConnectString(), 10000, CONNECTION_TIMEOUT_MS, null, new RetryOneTime(1));
        try
        {
            assertFalse(client.isConnected());
            client.start();

            outer: do
            {
                for ( int i = 0; i < (CONNECTION_TIMEOUT_MS / 1000); ++i )
                {
                    if ( client.isConnected() )
                    {
                        break outer;
                    }

                    Thread.sleep(CONNECTION_TIMEOUT_MS);
                }

                fail();
            } while ( false );
        }
        finally
        {
            client.close();
        }
    }
}
