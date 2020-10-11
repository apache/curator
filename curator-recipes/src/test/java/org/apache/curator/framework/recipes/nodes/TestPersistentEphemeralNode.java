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
package org.apache.curator.framework.recipes.nodes;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.imps.TestCleanState;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.compatibility.Timing2;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("deprecation")
public class TestPersistentEphemeralNode extends BaseClassForTests
{
    private static final Logger log = LoggerFactory.getLogger(TestPersistentEphemeralNode.class);
    private static final String DIR = "/test";
    private static final String PATH = ZKPaths.makePath(DIR, "/foo");

    private final Collection<CuratorFramework> curatorInstances = Lists.newArrayList();
    private final Collection<PersistentEphemeralNode> createdNodes = Lists.newArrayList();

    private final Timing2 timing = new Timing2();

    @AfterEach
    @Override
    public void teardown() throws Exception
    {
        try
        {
            for ( PersistentEphemeralNode node : createdNodes )
            {
                CloseableUtils.closeQuietly(node);
            }

            for ( CuratorFramework curator : curatorInstances )
            {
                TestCleanState.closeAndTestClean(curator);
            }
        }
        finally
        {
            super.teardown();
        }
    }

    @Test
    public void testListenersReconnectedIsFast() throws Exception
    {
        server.stop();

        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        try
        {
            client.start();
            try ( PersistentEphemeralNode node = new PersistentEphemeralNode(client, PersistentEphemeralNode.Mode.EPHEMERAL, "/abc/node", "hello".getBytes()) )
            {
                node.debugWaitMsForBackgroundBeforeClose.set(timing.forSleepingABit().milliseconds());
                node.start();

                final CountDownLatch connectedLatch = new CountDownLatch(1);
                final CountDownLatch reconnectedLatch = new CountDownLatch(1);
                ConnectionStateListener listener = new ConnectionStateListener()
                {
                    @Override
                    public void stateChanged(CuratorFramework client, ConnectionState newState)
                    {
                        if ( newState == ConnectionState.CONNECTED )
                        {
                            connectedLatch.countDown();
                        }
                        if ( newState == ConnectionState.RECONNECTED )
                        {
                            reconnectedLatch.countDown();
                        }
                    }
                };
                client.getConnectionStateListenable().addListener(listener);
                timing.sleepABit();
                server.restart();
                assertTrue(timing.awaitLatch(connectedLatch));
                timing.sleepABit();
                assertTrue(node.waitForInitialCreate(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS));
                server.stop();
                timing.sleepABit();
                server.restart();
                timing.sleepABit();
                assertTrue(timing.awaitLatch(reconnectedLatch));
            }
        }
        finally
        {
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
    public void testNoServerAtStart() throws Exception
    {
        server.stop();

        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        PersistentEphemeralNode node = null;
        try
        {
            client.start();
            node = new PersistentEphemeralNode(client, PersistentEphemeralNode.Mode.EPHEMERAL, "/abc/node", "hello".getBytes());
            node.debugWaitMsForBackgroundBeforeClose.set(timing.forSleepingABit().milliseconds());
            node.start();

            final CountDownLatch connectedLatch = new CountDownLatch(1);
            ConnectionStateListener listener = new ConnectionStateListener()
            {
                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState)
                {
                    if ( newState == ConnectionState.CONNECTED )
                    {
                        connectedLatch.countDown();
                    }
                }
            };
            client.getConnectionStateListenable().addListener(listener);

            timing.sleepABit();

            server.restart();

            assertTrue(timing.awaitLatch(connectedLatch));

            timing.sleepABit();

            assertTrue(node.waitForInitialCreate(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS));
        }
        finally
        {
            CloseableUtils.closeQuietly(node);
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
    public void testNullCurator()
    {
        assertThrows(NullPointerException.class, ()-> {
            new PersistentEphemeralNode(null, PersistentEphemeralNode.Mode.EPHEMERAL, PATH, new byte[0]);
        });
    }

    @Test
    public void testNullPath()
    {
        assertThrows(IllegalArgumentException.class, ()-> {
            CuratorFramework curator = newCurator();
            new PersistentEphemeralNode(curator, PersistentEphemeralNode.Mode.EPHEMERAL, null, new byte[0]);
        });
    }

    @Test
    public void testNullData()
    {
        assertThrows(NullPointerException.class, ()-> {
            CuratorFramework curator = newCurator();
            new PersistentEphemeralNode(curator, PersistentEphemeralNode.Mode.EPHEMERAL, PATH, null);
        });
    }

    @Test
    public void testNullMode()
    {
        assertThrows(NullPointerException.class, ()->{
            CuratorFramework curator = newCurator();
            new PersistentEphemeralNode(curator, null, PATH, new byte[0]);
            });
    }

    @Test
    public void testSettingDataSequential() throws Exception
    {
        setDataTest(PersistentEphemeralNode.Mode.EPHEMERAL_SEQUENTIAL);
    }

    @Test
    public void testSettingData() throws Exception
    {
        setDataTest(PersistentEphemeralNode.Mode.EPHEMERAL);
    }

    protected void setDataTest(PersistentEphemeralNode.Mode mode) throws Exception
    {
        PersistentEphemeralNode node = null;
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        try
        {
            client.start();
            node = new PersistentEphemeralNode(client, mode, PATH, "a".getBytes());
            node.debugWaitMsForBackgroundBeforeClose.set(timing.forSleepingABit().milliseconds());
            node.start();
            assertTrue(node.waitForInitialCreate(timing.forWaiting().seconds(), TimeUnit.SECONDS));

            assertArrayEquals(client.getData().forPath(node.getActualPath()), "a".getBytes());

            final Semaphore semaphore = new Semaphore(0);
            Watcher watcher = new Watcher()
            {
                @Override
                public void process(WatchedEvent arg0)
                {
                    semaphore.release();
                }
            };
            client.checkExists().usingWatcher(watcher).forPath(node.getActualPath());
            node.setData("b".getBytes());
            assertTrue(timing.acquireSemaphore(semaphore));
            assertEquals(node.getActualPath(), node.getActualPath());
            assertArrayEquals(client.getData().usingWatcher(watcher).forPath(node.getActualPath()), "b".getBytes());
            node.setData("c".getBytes());
            assertTrue(timing.acquireSemaphore(semaphore));
            assertEquals(node.getActualPath(), node.getActualPath());
            assertArrayEquals(client.getData().usingWatcher(watcher).forPath(node.getActualPath()), "c".getBytes());
            node.close();
            assertTrue(timing.acquireSemaphore(semaphore));
        }
        finally
        {
            CloseableUtils.closeQuietly(node);
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
    public void testDeletesNodeWhenClosed() throws Exception
    {
        CuratorFramework curator = newCurator();

        PersistentEphemeralNode node = new PersistentEphemeralNode(curator, PersistentEphemeralNode.Mode.EPHEMERAL, PATH, new byte[0]);
        node.debugWaitMsForBackgroundBeforeClose.set(timing.forSleepingABit().milliseconds());
        node.start();
        String path = null;
        try
        {
            node.waitForInitialCreate(5, TimeUnit.SECONDS);
            path = node.getActualPath();
            assertNodeExists(curator, path);
        }
        finally
        {
            CloseableUtils.closeQuietly(node);
        }

        assertNodeDoesNotExist(curator, path);
    }

    @Test
    public void testClosingMultipleTimes() throws Exception
    {
        CuratorFramework curator = newCurator();

        PersistentEphemeralNode node = new PersistentEphemeralNode(curator, PersistentEphemeralNode.Mode.EPHEMERAL, PATH, new byte[0]);
        node.debugWaitMsForBackgroundBeforeClose.set(timing.forSleepingABit().milliseconds());
        node.start();
        node.waitForInitialCreate(timing.forWaiting().seconds(), TimeUnit.SECONDS);

        String path = node.getActualPath();
        node.close();
        assertNodeDoesNotExist(curator, path);

        node.close();
        assertNodeDoesNotExist(curator, path);
    }

    @Test
    public void testDeletesNodeWhenSessionDisconnects() throws Exception
    {
        CuratorFramework curator = newCurator();
        CuratorFramework observer = newCurator();

        PersistentEphemeralNode node = new PersistentEphemeralNode(curator, PersistentEphemeralNode.Mode.EPHEMERAL, PATH, new byte[0]);
        node.debugWaitMsForBackgroundBeforeClose.set(timing.forSleepingABit().milliseconds());
        try
        {
            node.start();
            node.waitForInitialCreate(timing.forWaiting().seconds(), TimeUnit.SECONDS);
            assertNodeExists(observer, node.getActualPath());

            // Register a watch that will fire when the node is deleted...
            Trigger deletedTrigger = Trigger.deletedOrSetData();
            observer.checkExists().usingWatcher(deletedTrigger).forPath(node.getActualPath());

            node.debugCreateNodeLatch = new CountDownLatch(1);
            curator.getZookeeperClient().getZooKeeper().getTestable().injectSessionExpiration();

            // Make sure the node got deleted
            assertTrue(deletedTrigger.firedWithin(timing.forSessionSleep().seconds(), TimeUnit.SECONDS));
            node.debugCreateNodeLatch.countDown();
        }
        finally
        {
            CloseableUtils.closeQuietly(node);
        }
    }

    @Test
    public void testRecreatesNodeWhenSessionReconnects() throws Exception
    {
        CuratorFramework curator = newCurator();
        CuratorFramework observer = newCurator();

        PersistentEphemeralNode node = new PersistentEphemeralNode(curator, PersistentEphemeralNode.Mode.EPHEMERAL, PATH, new byte[0]);
        node.debugWaitMsForBackgroundBeforeClose.set(timing.forSleepingABit().milliseconds());
        try
        {
            node.start();
            node.waitForInitialCreate(5, TimeUnit.SECONDS);
            assertNodeExists(observer, node.getActualPath());

            Trigger deletedTrigger = Trigger.deletedOrSetData();
            observer.checkExists().usingWatcher(deletedTrigger).forPath(node.getActualPath());

            node.debugCreateNodeLatch = new CountDownLatch(1);
            curator.getZookeeperClient().getZooKeeper().getTestable().injectSessionExpiration();

            // Make sure the node got deleted...
            assertTrue(deletedTrigger.firedWithin(timing.forSessionSleep().seconds(), TimeUnit.SECONDS));
            node.debugCreateNodeLatch.countDown();

            // Check for it to be recreated...
            Trigger createdTrigger = Trigger.created();
            Stat stat = observer.checkExists().usingWatcher(createdTrigger).forPath(node.getActualPath());
            assertTrue(stat != null || createdTrigger.firedWithin(timing.forWaiting().seconds(), TimeUnit.SECONDS));
        }
        finally
        {
            CloseableUtils.closeQuietly(node);
        }
    }

    @Test
    public void testRecreatesNodeWhenSessionReconnectsMultipleTimes() throws Exception
    {
        CuratorFramework curator = newCurator();
        CuratorFramework observer = newCurator();

        PersistentEphemeralNode node = new PersistentEphemeralNode(curator, PersistentEphemeralNode.Mode.EPHEMERAL, PATH, new byte[0]);
        node.debugWaitMsForBackgroundBeforeClose.set(timing.forSleepingABit().milliseconds());
        try
        {
            node.start();
            node.waitForInitialCreate(timing.forWaiting().seconds(), TimeUnit.SECONDS);
            String path = node.getActualPath();
            assertNodeExists(observer, path);

            // We should be able to disconnect multiple times and each time the node should be recreated.
            for ( int i = 0; i < 5; i++ )
            {
                Trigger deletionTrigger = Trigger.deletedOrSetData();
                Stat stat = observer.checkExists().usingWatcher(deletionTrigger).forPath(path);
                assertNotNull(stat, "node should exist: " + path);

                node.debugCreateNodeLatch = new CountDownLatch(1);
                // Kill the session, thus cleaning up the node...
                curator.getZookeeperClient().getZooKeeper().getTestable().injectSessionExpiration();

                // Make sure the node ended up getting deleted...
                assertTrue(deletionTrigger.firedWithin(timing.multiple(1.5).forSessionSleep().seconds(), TimeUnit.SECONDS));
                node.debugCreateNodeLatch.countDown();

                // Now put a watch in the background looking to see if it gets created...
                Trigger creationTrigger = Trigger.created();
                stat = observer.checkExists().usingWatcher(creationTrigger).forPath(path);
                assertTrue(stat != null || creationTrigger.firedWithin(timing.forWaiting().seconds(), TimeUnit.SECONDS));
            }
        }
        finally
        {
            CloseableUtils.closeQuietly(node);
        }
    }

    @Test
    public void testRecreatesNodeWhenEphemeralOwnerSessionExpires() throws Exception
    {
        CuratorFramework curator = newCurator();
        CuratorFramework nodeCreator = newCurator();
        CuratorFramework observer = newCurator();

        nodeCreator.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(PATH, new byte[0]);

        Trigger dataChangedTrigger = Trigger.dataChanged();
        observer.getData().usingWatcher(dataChangedTrigger).forPath(PATH);

        PersistentEphemeralNode node = new PersistentEphemeralNode(curator, PersistentEphemeralNode.Mode.EPHEMERAL, PATH, new byte[0]);
        node.debugWaitMsForBackgroundBeforeClose.set(timing.forSleepingABit().milliseconds());
        node.start();
        try
        {
            node.waitForInitialCreate(5, TimeUnit.SECONDS);
            assertNodeExists(observer, node.getActualPath());

            assertTrue(dataChangedTrigger.firedWithin(timing.forWaiting().seconds(), TimeUnit.SECONDS));

            Trigger deletedTrigger = Trigger.deletedOrSetData();
            observer.checkExists().usingWatcher(deletedTrigger).forPath(node.getActualPath());

            nodeCreator.getZookeeperClient().getZooKeeper().getTestable().injectSessionExpiration();

            // Make sure the node got deleted...
            assertTrue(deletedTrigger.firedWithin(timing.forWaiting().seconds(), TimeUnit.SECONDS));

            // Check for it to be recreated...
            Trigger createdTrigger = Trigger.created();
            Stat stat = observer.checkExists().usingWatcher(createdTrigger).forPath(node.getActualPath());
            assertTrue(stat != null || createdTrigger.firedWithin(timing.forWaiting().seconds(), TimeUnit.SECONDS));
        }
        finally
        {
            node.close();
        }
    }

    @Test
    public void testRecreatesNodeWhenItGetsDeleted() throws Exception
    {
        CuratorFramework curator = newCurator();

        PersistentEphemeralNode node = new PersistentEphemeralNode(curator, PersistentEphemeralNode.Mode.EPHEMERAL, PATH, new byte[0]);
        node.debugWaitMsForBackgroundBeforeClose.set(timing.forSleepingABit().milliseconds());
        try
        {
            node.start();
            node.waitForInitialCreate(timing.forWaiting().seconds(), TimeUnit.SECONDS);
            String originalNode = node.getActualPath();
            assertNodeExists(curator, originalNode);

            // Delete the original node...
            curator.delete().forPath(originalNode);

            // Since we're using an ephemeral node, and the original session hasn't been interrupted the name of the new
            // node that gets created is going to be exactly the same as the original.
            Trigger createdWatchTrigger = Trigger.created();
            Stat stat = curator.checkExists().usingWatcher(createdWatchTrigger).forPath(originalNode);
            assertTrue(stat != null || createdWatchTrigger.firedWithin(timing.forWaiting().seconds(), TimeUnit.SECONDS));
        }
        finally
        {
            CloseableUtils.closeQuietly(node);
        }
    }

    @Test
    public void testNodesCreateUniquePaths() throws Exception
    {
        CuratorFramework curator = newCurator();

        try ( PersistentEphemeralNode node1 = new PersistentEphemeralNode(curator, PersistentEphemeralNode.Mode.EPHEMERAL_SEQUENTIAL, PATH, new byte[0]) )
        {
            node1.debugWaitMsForBackgroundBeforeClose.set(timing.forSleepingABit().milliseconds());
            node1.start();
            node1.waitForInitialCreate(timing.forWaiting().seconds(), TimeUnit.SECONDS);
            String path1 = node1.getActualPath();

            PersistentEphemeralNode node2 = new PersistentEphemeralNode(curator, PersistentEphemeralNode.Mode.EPHEMERAL_SEQUENTIAL, PATH, new byte[0]);
            node2.debugWaitMsForBackgroundBeforeClose.set(timing.forSleepingABit().milliseconds());
            node2.start();
            try
            {
                node2.waitForInitialCreate(timing.forWaiting().seconds(), TimeUnit.SECONDS);
                String path2 = node2.getActualPath();

                assertFalse(path1.equals(path2));
            }
            finally
            {
                node2.close();
            }
        }
    }

    @Test
    public void testData() throws Exception
    {
        CuratorFramework curator = newCurator();
        byte[] data = "Hello World".getBytes();

        PersistentEphemeralNode node = new PersistentEphemeralNode(curator, PersistentEphemeralNode.Mode.EPHEMERAL, PATH, data);
        node.debugWaitMsForBackgroundBeforeClose.set(timing.forSleepingABit().milliseconds());
        try
        {
            node.start();
            node.waitForInitialCreate(timing.forWaiting().seconds(), TimeUnit.SECONDS);
            assertTrue(Arrays.equals(curator.getData().forPath(node.getActualPath()), data));
        }
        finally
        {
            CloseableUtils.closeQuietly(node);
        }
    }

    /**
     * Test that if a persistent ephemeral node is created and the node already exists
     * that if data is present in the PersistentEphermalNode that it is still set.
     * @throws Exception
     */
    @Test
    public void testSetDataWhenNodeExists() throws Exception
    {
        CuratorFramework curator = newCurator();
        curator.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(PATH, "InitialData".getBytes());

        byte[] data = "Hello World".getBytes();

        PersistentEphemeralNode node = new PersistentEphemeralNode(curator, PersistentEphemeralNode.Mode.EPHEMERAL, PATH, data);
        node.debugWaitMsForBackgroundBeforeClose.set(timing.forSleepingABit().milliseconds());
        try
        {
            node.start();
            node.waitForInitialCreate(timing.forWaiting().seconds(), TimeUnit.SECONDS);
            assertTrue(Arrays.equals(curator.getData().forPath(node.getActualPath()), data));
        }
        finally
        {
            CloseableUtils.closeQuietly(node);
        }
    }

    @Test
    public void testSetDataWhenDisconnected() throws Exception
    {
        CuratorFramework curator = newCurator();

        byte[] initialData = "Hello World".getBytes();
        byte[] updatedData = "Updated".getBytes();

        PersistentEphemeralNode node = new PersistentEphemeralNode(curator, PersistentEphemeralNode.Mode.EPHEMERAL, PATH, initialData);
        try
        {
            node.debugWaitMsForBackgroundBeforeClose.set(timing.forSleepingABit().milliseconds());
            node.start();
            node.waitForInitialCreate(timing.forWaiting().seconds(), TimeUnit.SECONDS);
            assertTrue(Arrays.equals(curator.getData().forPath(node.getActualPath()), initialData));

            server.stop();

            final CountDownLatch dataUpdateLatch = new CountDownLatch(1);

            Watcher watcher = new Watcher()
            {
				@Override
				public void process(WatchedEvent event)
				{
					if ( event.getType() == EventType.NodeDataChanged )
					{
						dataUpdateLatch.countDown();
					}
				}
            };

            curator.getData().usingWatcher(watcher).inBackground().forPath(node.getActualPath());

            node.setData(updatedData);
            server.restart();

            assertTrue(timing.awaitLatch(dataUpdateLatch));

            assertTrue(Arrays.equals(curator.getData().forPath(node.getActualPath()), updatedData));
        }
        finally
        {
            CloseableUtils.closeQuietly(node);
        }
    }

    @Test
    public void testSetUpdatedDataWhenReconnected() throws Exception
    {
        CuratorFramework curator = newCurator();

        byte[] initialData = "Hello World".getBytes();
        byte[] updatedData = "Updated".getBytes();

        PersistentEphemeralNode node = new PersistentEphemeralNode(curator, PersistentEphemeralNode.Mode.EPHEMERAL, PATH, initialData);
        try
        {
            node.debugWaitMsForBackgroundBeforeClose.set(timing.forSleepingABit().milliseconds());
            node.start();
            node.waitForInitialCreate(timing.forWaiting().seconds(), TimeUnit.SECONDS);
            assertTrue(Arrays.equals(curator.getData().forPath(node.getActualPath()), initialData));

            node.setData(updatedData);
            assertTrue(Arrays.equals(curator.getData().forPath(node.getActualPath()), updatedData));

            server.restart();

            final CountDownLatch dataUpdateLatch = new CountDownLatch(1);
            curator.getData().inBackground(new BackgroundCallback() {

                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                    dataUpdateLatch.countDown();
                }
            }).forPath(node.getActualPath());

            assertTrue(timing.awaitLatch(dataUpdateLatch));

            assertTrue(Arrays.equals(curator.getData().forPath(node.getActualPath()), updatedData));
        }
        finally
        {
            CloseableUtils.closeQuietly(node);
        }
    }

    /**
     * See CURATOR-190
     * For protected nodes on reconnect the current protected name was passed to the create builder meaning that it got
     * appended to the new protected node name. This meant that a new node got created on each reconnect.
     * @throws Exception
     */
    @Test
    public void testProtected() throws Exception
    {
        CuratorFramework curator = newCurator();

        PersistentEphemeralNode node = new PersistentEphemeralNode(curator, PersistentEphemeralNode.Mode.PROTECTED_EPHEMERAL, PATH,
                                                                   new byte[0]);
        try
        {
            node.debugWaitMsForBackgroundBeforeClose.set(timing.forSleepingABit().milliseconds());
            node.start();
            node.waitForInitialCreate(timing.forWaiting().seconds(), TimeUnit.SECONDS);
            assertNodeExists(curator, node.getActualPath());

            server.restart();

            curator.blockUntilConnected(5, TimeUnit.SECONDS);

            assertNodeExists(curator, node.getActualPath());

            //There should only be a single child, the persisted ephemeral node
            List<String> children = curator.getChildren().forPath(DIR);
            assertFalse(children == null);
            assertEquals(children.size(), 1);
        }
        finally
        {
            CloseableUtils.closeQuietly(node);
        }
    }

    @Test
    public void testNoCreatePermission() throws Exception
    {
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        CuratorFramework client = builder
            .connectString(server.getConnectString())
            .authorization("digest", "me1:pass1".getBytes())
            .retryPolicy(new RetryOneTime(1))
            .build();

        PersistentEphemeralNode node = null;
        try {
            client.start();

            ACL acl = new ACL(ZooDefs.Perms.WRITE, ZooDefs.Ids.AUTH_IDS);
            List<ACL> aclList = Lists.newArrayList(acl);
            client.create().withACL(aclList).forPath(DIR, new byte[0]);
            client.close();

        	//New client without authentication
        	client = newCurator();

        	node = new PersistentEphemeralNode(client, PersistentEphemeralNode.Mode.EPHEMERAL, PATH,
                                                                   new byte[0]);
            node.debugWaitMsForBackgroundBeforeClose.set(timing.forSleepingABit().milliseconds());
        	node.start();

            node.waitForInitialCreate(timing.seconds(), TimeUnit.SECONDS);
            assertNodeDoesNotExist(client, PATH);
            assertTrue(node.isAuthFailure());
        } finally {
            CloseableUtils.closeQuietly(node);
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testNoWritePermission() throws Exception
    {
        final ACLProvider aclProvider = new ACLProvider() {
            final ACL acl = new ACL(ZooDefs.Perms.READ | ZooDefs.Perms.CREATE | ZooDefs.Perms.DELETE, ZooDefs.Ids.ANYONE_ID_UNSAFE);
            final List<ACL> aclList = Collections.singletonList(acl);
            @Override
            public List<ACL> getDefaultAcl() {
                return aclList;
            }

            @Override
            public List<ACL> getAclForPath(String path) {
                return aclList;
            }
        };

        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        CuratorFramework client = builder
                .connectString(server.getConnectString())
                .aclProvider(aclProvider)
                .retryPolicy(new RetryOneTime(1))
                .build();

        PersistentEphemeralNode node = null;
        try {
            client.start();

            node = new PersistentEphemeralNode(client, PersistentEphemeralNode.Mode.EPHEMERAL, PATH,
                    new byte[0]);
            node.start();

            assertTrue(node.waitForInitialCreate(timing.seconds(), TimeUnit.SECONDS), "Node not created");
            assertNodeExists(client, PATH);
            assertFalse(node.isAuthFailure(), "AuthFailure when creating node.");

            byte[] NEW_DATA = "NEW_DATA".getBytes();
            node.setData(NEW_DATA);
            timing.sleepABit();
            byte[] read_data = client.getData().forPath(PATH);
            assertNotEquals(read_data, NEW_DATA, "Data matches - write went through.");
            assertTrue(node.isAuthFailure(), "AuthFailure response not received.");
        } finally {
            CloseableUtils.closeQuietly(node);
            CloseableUtils.closeQuietly(client);
        }
    }

    private void assertNodeExists(CuratorFramework curator, String path) throws Exception
    {
        assertNotNull(path);
        assertTrue(curator.checkExists().forPath(path) != null);
    }

    private void assertNodeDoesNotExist(CuratorFramework curator, String path) throws Exception
    {
        assertTrue(curator.checkExists().forPath(path) == null);
    }

    private CuratorFramework newCurator() throws IOException
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        client.start();

        curatorInstances.add(client);
        return client;
    }

    private static final class Trigger implements Watcher
    {
        private final Set<EventType> types;
        private final CountDownLatch latch;

        public Trigger(Event.EventType... types)
        {
            assertNotNull(types);

            this.types = ImmutableSet.copyOf(types);
            this.latch = new CountDownLatch(1);
        }

        @Override
        public void process(WatchedEvent event)
        {
            if ( types.contains(event.getType()) )
            {
                latch.countDown();
            }
            else if ( event.getType() != EventType.None )
            {
                log.warn("Unexpected watcher event: " + event);
            }
        }

        public boolean firedWithin(long duration, TimeUnit unit)
        {
            try
            {
                return latch.await(duration, unit);
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
                throw Throwables.propagate(e);
            }
        }

        private static Trigger created()
        {
            return new Trigger(Event.EventType.NodeCreated);
        }

        private static Trigger deletedOrSetData()
        {
            return new Trigger(Event.EventType.NodeDeleted, EventType.NodeDataChanged);
        }

        private static Trigger dataChanged()
        {
            return new Trigger(EventType.NodeDataChanged);
        }
    }
}
