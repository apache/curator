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

package org.apache.curator.framework.recipes.watch;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.compatibility.CuratorTestBase;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

@Test(groups = CuratorTestBase.zk36Group)
public class TestPersistentWatcher extends CuratorTestBase
{
    @Test
    public void testConnectionLostRecursive() throws Exception
    {
        internalTest(true);
    }

    @Test
    public void testConnectionLost() throws Exception
    {
        internalTest(false);
    }

    private void internalTest(boolean recursive) throws Exception
    {
        try ( CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1)) )
        {
            CountDownLatch lostLatch = new CountDownLatch(1);
            CountDownLatch reconnectedLatch = new CountDownLatch(1);
            client.start();
            client.getConnectionStateListenable().addListener((__, newState) -> {
                if ( newState == ConnectionState.LOST )
                {
                    lostLatch.countDown();
                }
                else if ( newState == ConnectionState.RECONNECTED )
                {
                    reconnectedLatch.countDown();
                }
            });

            try ( PersistentWatcher persistentWatcher = new PersistentWatcher(client, "/top/main", recursive) )
            {
                persistentWatcher.start();

                BlockingQueue<WatchedEvent> events = new LinkedBlockingQueue<>();
                persistentWatcher.getListenable().addListener(events::add);

                client.create().creatingParentsIfNeeded().forPath("/top/main/a");
                Assert.assertEquals(timing.takeFromQueue(events).getPath(), "/top/main");
                if ( recursive )
                {
                    Assert.assertEquals(timing.takeFromQueue(events).getPath(), "/top/main/a");
                }
                else
                {
                    Assert.assertEquals(timing.takeFromQueue(events).getPath(), "/top/main");   // child added
                }

                server.stop();
                Assert.assertEquals(timing.takeFromQueue(events).getState(), Watcher.Event.KeeperState.Disconnected);
                Assert.assertTrue(timing.awaitLatch(lostLatch));

                server.restart();
                Assert.assertTrue(timing.awaitLatch(reconnectedLatch));

                timing.sleepABit();     // time to allow watcher to get reset
                events.clear();

                if ( recursive )
                {
                    client.setData().forPath("/top/main/a", "foo".getBytes());
                    Assert.assertEquals(timing.takeFromQueue(events).getType(), Watcher.Event.EventType.NodeDataChanged);
                }
                client.setData().forPath("/top/main", "bar".getBytes());
                Assert.assertEquals(timing.takeFromQueue(events).getPath(), "/top/main");
            }
        }
    }
}