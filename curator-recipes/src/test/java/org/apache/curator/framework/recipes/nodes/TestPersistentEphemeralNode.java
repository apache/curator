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

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.TestCleanState;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.KillSession;
import org.apache.curator.test.TestingServer;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.*;

public class TestPersistentEphemeralNode extends BaseClassForTests
{
    private static final String DIR = "/test";
    private static final String PATH = ZKPaths.makePath(DIR, "/foo");

    private final Collection<CuratorFramework> curatorInstances = Lists.newArrayList();
    private final Collection<PersistentEphemeralNode> createdNodes = Lists.newArrayList();

    private final Timing timing = new Timing();

    @AfterMethod
    @Override
    public void teardown() throws Exception
    {
        for ( PersistentEphemeralNode node : createdNodes )
        {
            node.close();
        }

        for ( CuratorFramework curator : curatorInstances )
        {
            TestCleanState.closeAndTestClean(curator);
        }
    }

    @Test
    public void testListenersReconnectedIsFast() throws Exception
    {
        server.close();

        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        try
        {
            client.start();
            PersistentEphemeralNode node = new PersistentEphemeralNode(client, PersistentEphemeralNode.Mode.EPHEMERAL, "/abc/node", "hello".getBytes());
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
            server = new TestingServer(server.getPort());
            Assert.assertTrue(timing.awaitLatch(connectedLatch));
            timing.sleepABit();
            Assert.assertTrue(node.waitForInitialCreate(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS));
            server.close();
            timing.sleepABit();
            server = new TestingServer(server.getPort());
            timing.sleepABit();
            Assert.assertTrue(timing.awaitLatch(reconnectedLatch));
        }
        finally
        {
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
    public void testNoServerAtStart() throws Exception
    {
        server.close();

        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        PersistentEphemeralNode node = null;
        try
        {
            client.start();
            node = new PersistentEphemeralNode(client, PersistentEphemeralNode.Mode.EPHEMERAL, "/abc/node", "hello".getBytes());
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

            server = new TestingServer(server.getPort());

            Assert.assertTrue(timing.awaitLatch(connectedLatch));

            timing.sleepABit();

            Assert.assertTrue(node.waitForInitialCreate(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS));
        }
        finally
        {
            CloseableUtils.closeQuietly(node);
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testNullCurator() throws Exception
    {
        new PersistentEphemeralNode(null, PersistentEphemeralNode.Mode.EPHEMERAL, PATH, new byte[0]);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNullPath() throws Exception
    {
        CuratorFramework curator = newCurator();
        new PersistentEphemeralNode(curator, PersistentEphemeralNode.Mode.EPHEMERAL, null, new byte[0]);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testNullData() throws Exception
    {
        CuratorFramework curator = newCurator();
        new PersistentEphemeralNode(curator, PersistentEphemeralNode.Mode.EPHEMERAL, PATH, null);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testNullMode() throws Exception
    {
        CuratorFramework curator = newCurator();
        new PersistentEphemeralNode(curator, null, PATH, new byte[0]);
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
            node.start();
            Assert.assertTrue(node.waitForInitialCreate(timing.forWaiting().seconds(), TimeUnit.SECONDS));

            Assert.assertEquals(client.getData().forPath(node.getActualPath()), "a".getBytes());

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
            Assert.assertTrue(timing.acquireSemaphore(semaphore));
            Assert.assertEquals(node.getActualPath(), node.getActualPath());
            Assert.assertEquals(client.getData().usingWatcher(watcher).forPath(node.getActualPath()), "b".getBytes());
            node.setData("c".getBytes());
            Assert.assertTrue(timing.acquireSemaphore(semaphore));
            Assert.assertEquals(node.getActualPath(), node.getActualPath());
            Assert.assertEquals(client.getData().usingWatcher(watcher).forPath(node.getActualPath()), "c".getBytes());
            node.close();
            Assert.assertTrue(timing.acquireSemaphore(semaphore));
        }
        finally
        {
            if ( node != null )
            {
                node.close();
            }
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
    public void testDeletesNodeWhenClosed() throws Exception
    {
        CuratorFramework curator = newCurator();

        PersistentEphemeralNode node = new PersistentEphemeralNode(curator, PersistentEphemeralNode.Mode.EPHEMERAL, PATH, new byte[0]);
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
            node.close();  // After closing the path is set to null...
        }

        assertNodeDoesNotExist(curator, path);
    }

    @Test
    public void testClosingMultipleTimes() throws Exception
    {
        CuratorFramework curator = newCurator();

        PersistentEphemeralNode node = new PersistentEphemeralNode(curator, PersistentEphemeralNode.Mode.EPHEMERAL, PATH, new byte[0]);
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
        node.start();
        try
        {
            node.waitForInitialCreate(timing.forWaiting().seconds(), TimeUnit.SECONDS);
            assertNodeExists(observer, node.getActualPath());

            // Register a watch that will fire when the node is deleted...
            Trigger deletedTrigger = Trigger.deleted();
            observer.checkExists().usingWatcher(deletedTrigger).forPath(node.getActualPath());

            killSession(curator);

            // Make sure the node got deleted
            assertTrue(deletedTrigger.firedWithin(timing.forWaiting().seconds(), TimeUnit.SECONDS));
        }
        finally
        {
            node.close();
        }
    }

    @Test
    public void testRecreatesNodeWhenSessionReconnects() throws Exception
    {
        CuratorFramework curator = newCurator();
        CuratorFramework observer = newCurator();

        PersistentEphemeralNode node = new PersistentEphemeralNode(curator, PersistentEphemeralNode.Mode.EPHEMERAL, PATH, new byte[0]);
        node.start();
        try
        {
            node.waitForInitialCreate(5, TimeUnit.SECONDS);
            assertNodeExists(observer, node.getActualPath());

            Trigger deletedTrigger = Trigger.deleted();
            observer.checkExists().usingWatcher(deletedTrigger).forPath(node.getActualPath());

            killSession(curator);

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
    public void testRecreatesNodeWhenSessionReconnectsMultipleTimes() throws Exception
    {
        CuratorFramework curator = newCurator();
        CuratorFramework observer = newCurator();

        PersistentEphemeralNode node = new PersistentEphemeralNode(curator, PersistentEphemeralNode.Mode.EPHEMERAL, PATH, new byte[0]);
        node.start();
        try
        {
            node.waitForInitialCreate(timing.forWaiting().seconds(), TimeUnit.SECONDS);
            String path = node.getActualPath();
            assertNodeExists(observer, path);

            // We should be able to disconnect multiple times and each time the node should be recreated.
            for ( int i = 0; i < 5; i++ )
            {
                Trigger deletionTrigger = Trigger.deleted();
                observer.checkExists().usingWatcher(deletionTrigger).forPath(path);

                // Kill the session, thus cleaning up the node...
                killSession(curator);

                // Make sure the node ended up getting deleted...
                assertTrue(deletionTrigger.firedWithin(timing.forWaiting().seconds(), TimeUnit.SECONDS));

                // Now put a watch in the background looking to see if it gets created...
                Trigger creationTrigger = Trigger.created();
                Stat stat = observer.checkExists().usingWatcher(creationTrigger).forPath(path);
                assertTrue(stat != null || creationTrigger.firedWithin(timing.forWaiting().seconds(), TimeUnit.SECONDS));
            }
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
        node.start();
        try
        {
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
            node.close();
        }
    }

    @Test
    public void testNodesCreateUniquePaths() throws Exception
    {
        CuratorFramework curator = newCurator();

        PersistentEphemeralNode node1 = new PersistentEphemeralNode(curator, PersistentEphemeralNode.Mode.EPHEMERAL_SEQUENTIAL, PATH, new byte[0]);
        node1.start();
        try
        {
            node1.waitForInitialCreate(timing.forWaiting().seconds(), TimeUnit.SECONDS);
            String path1 = node1.getActualPath();

            PersistentEphemeralNode node2 = new PersistentEphemeralNode(curator, PersistentEphemeralNode.Mode.EPHEMERAL_SEQUENTIAL, PATH, new byte[0]);
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
        finally
        {
            node1.close();
        }
    }

    @Test
    public void testData() throws Exception
    {
        CuratorFramework curator = newCurator();
        byte[] data = "Hello World".getBytes();

        PersistentEphemeralNode node = new PersistentEphemeralNode(curator, PersistentEphemeralNode.Mode.EPHEMERAL, PATH, data);
        node.start();
        try
        {
            node.waitForInitialCreate(timing.forWaiting().seconds(), TimeUnit.SECONDS);
            assertTrue(Arrays.equals(curator.getData().forPath(node.getActualPath()), data));
        }
        finally
        {
            node.close();
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

    public void killSession(CuratorFramework curator) throws Exception
    {
        KillSession.kill(curator.getZookeeperClient().getZooKeeper(), curator.getZookeeperClient().getCurrentConnectionString());
    }

    private static final class Trigger implements Watcher
    {
        private final Event.EventType type;
        private final CountDownLatch latch;

        public Trigger(Event.EventType type)
        {
            assertNotNull(type);

            this.type = type;
            this.latch = new CountDownLatch(1);
        }

        @Override
        public void process(WatchedEvent event)
        {
            if ( type == event.getType() )
            {
                latch.countDown();
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
                throw Throwables.propagate(e);
            }
        }

        private static Trigger created()
        {
            return new Trigger(Event.EventType.NodeCreated);
        }

        private static Trigger deleted()
        {
            return new Trigger(Event.EventType.NodeDeleted);
        }
    }
}
