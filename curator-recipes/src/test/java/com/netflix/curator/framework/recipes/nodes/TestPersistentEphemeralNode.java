/*
 * Copyright 2012 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.curator.framework.recipes.nodes;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.recipes.BaseClassForTests;
import com.netflix.curator.retry.RetryOneTime;
import com.netflix.curator.test.KillSession;
import com.netflix.curator.test.TestingServer;
import com.netflix.curator.test.Timing;
import com.netflix.curator.utils.ZKPaths;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.*;

public class TestPersistentEphemeralNode extends BaseClassForTests
{
    private static final String DIR = "/test";
    private static final String PATH = ZKPaths.makePath(DIR, "/foo");

    private final Collection<CuratorFramework> clients = Lists.newArrayList();
    private final Collection<PersistentEphemeralNode> createdNodes = Lists.newArrayList();

    @AfterMethod
    public void teardown() throws Exception
    {
        for ( PersistentEphemeralNode node : createdNodes )
        {
            Closeables.closeQuietly(node);
        }

        for ( CuratorFramework curator : clients )
        {
            Closeables.closeQuietly(curator);
        }

        super.teardown();
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testNullCurator() throws Exception
    {
        new PersistentEphemeralNode(null, PersistentEphemeralNode.Mode.PROTECTED_EPHEMERAL, PATH, new byte[0]);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testNullPath() throws Exception
    {
        CuratorFramework curator = newCurator();
        new PersistentEphemeralNode(curator, PersistentEphemeralNode.Mode.PROTECTED_EPHEMERAL, null, new byte[0]);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testNullData() throws Exception
    {
        CuratorFramework curator = newCurator();
        new PersistentEphemeralNode(curator, PersistentEphemeralNode.Mode.PROTECTED_EPHEMERAL, PATH, null);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testNullMode() throws Exception
    {
        CuratorFramework curator = newCurator();
        new PersistentEphemeralNode(curator, null, PATH, new byte[0]);
    }

    @Test
    public void testCreatesNodeOnConstruction() throws Exception
    {
        CuratorFramework curator = newCurator();

        PersistentEphemeralNode node = new PersistentEphemeralNode(curator, PersistentEphemeralNode.Mode.PROTECTED_EPHEMERAL, PATH, new byte[0]);
        node.start();
        node.waitForInitialCreate(10, TimeUnit.SECONDS);
        assertNodeExists(curator, node.getActualPath());
    }

    @Test
    public void testDeletesNodeWhenClosed() throws Exception
    {
        CuratorFramework curator = newCurator();

        PersistentEphemeralNode node = new PersistentEphemeralNode(curator, PersistentEphemeralNode.Mode.PROTECTED_EPHEMERAL, PATH, new byte[0]);
        node.start();
        node.waitForInitialCreate(10, TimeUnit.SECONDS);
        assertNodeExists(curator, node.getActualPath());

        String path = node.getActualPath();
        node.close();  // After closing the path is set to null...
        assertNodeDoesNotExist(curator, path);
    }

    @Test
    public void testClosingMultipleTimes() throws Exception
    {
        CuratorFramework curator = newCurator();

        PersistentEphemeralNode node = new PersistentEphemeralNode(curator, PersistentEphemeralNode.Mode.PROTECTED_EPHEMERAL, PATH, new byte[0]);
        node.start();
        node.waitForInitialCreate(10, TimeUnit.SECONDS);

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

        PersistentEphemeralNode node = new PersistentEphemeralNode(curator, PersistentEphemeralNode.Mode.PROTECTED_EPHEMERAL, PATH, new byte[0]);
        node.start();
        node.waitForInitialCreate(10, TimeUnit.SECONDS);
        assertNodeExists(observer, node.getActualPath());

        // Register a watch that will fire when the node is deleted...
        Trigger deletedTrigger = Trigger.deleted();
        observer.checkExists().usingWatcher(deletedTrigger).forPath(node.getActualPath());

        killSession(curator);

        // Make sure the node got deleted
        assertTrue(deletedTrigger.firedWithin(10, TimeUnit.SECONDS));
    }

    @Test
    public void testRecreatesNodeWhenSessionReconnects() throws Exception
    {
        CuratorFramework curator = newCurator();
        CuratorFramework observer = newCurator();

        PersistentEphemeralNode node = new PersistentEphemeralNode(curator, PersistentEphemeralNode.Mode.PROTECTED_EPHEMERAL, PATH, new byte[0]);
        node.start();
        node.waitForInitialCreate(10, TimeUnit.SECONDS);
        String actualPath = node.getActualPath();
        assertNodeExists(observer, actualPath);

        Trigger deletedTrigger = Trigger.deleted();
        observer.checkExists().usingWatcher(deletedTrigger).forPath(actualPath);

        killSession(curator);

        // Make sure the node got deleted...
        assertTrue(deletedTrigger.firedWithin(10, TimeUnit.SECONDS));

        // Check for it to be recreated...
        Trigger createdTrigger = Trigger.created();
        Stat stat = observer.checkExists().usingWatcher(createdTrigger).forPath(actualPath);
        assertTrue(stat != null || createdTrigger.firedWithin(10, TimeUnit.SECONDS));
    }

    @Test
    public void testRecreatesNodeWhenSessionReconnectsMultipleTimes() throws Exception
    {
        CuratorFramework curator = newCurator();
        CuratorFramework observer = newCurator();

        PersistentEphemeralNode node = new PersistentEphemeralNode(curator, PersistentEphemeralNode.Mode.PROTECTED_EPHEMERAL, PATH, new byte[0]);
        node.start();
        node.waitForInitialCreate(10, TimeUnit.SECONDS);
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
            assertTrue(deletionTrigger.firedWithin(10, TimeUnit.SECONDS));

            // Now put a watch in the background looking to see if it gets created...
            Trigger creationTrigger = Trigger.created();
            Stat stat = observer.checkExists().usingWatcher(creationTrigger).forPath(path);
            assertTrue(stat != null || creationTrigger.firedWithin(10, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testSafeRecreatesNodeWhenReconnects() throws Exception
    {
        CuratorFramework curator = newCurator();

        PersistentEphemeralNode node = new PersistentEphemeralNode(curator, PersistentEphemeralNode.Mode.PROTECTED_EPHEMERAL, PATH, new byte[0]);
        node.start();
        node.waitForInitialCreate(10, TimeUnit.SECONDS);
        String path = node.getActualPath();

        server.stop();
        node.initialCreateLatch = new CountDownLatch(1);
        server = new TestingServer(server.getPort(), server.getTempDirectory());
        assertTrue(node.waitForInitialCreate(10, TimeUnit.SECONDS));
        String postPath = node.getActualPath();
        assertNodeExists(curator, postPath);
        assertEquals(path, postPath);
    }

    @Test
    public void testSequentialNodeRecreate() throws Exception
    {
        CuratorFramework curator = newCurator();

        PersistentEphemeralNode node = new PersistentEphemeralNode(curator, PersistentEphemeralNode.Mode.EPHEMERAL_SEQUENTIAL, PATH, new byte[0]);
        node.start();
        node.waitForInitialCreate(10, TimeUnit.SECONDS);
        String originalNode = node.getActualPath();
        assertNodeExists(curator, originalNode);

        node.initialCreateLatch = new CountDownLatch(1);
        curator.delete().forPath(originalNode);

        node.waitForInitialCreate(10, TimeUnit.SECONDS);

        String newNode = node.getActualPath();
        assertNodeDoesNotExist(curator, originalNode);
        assertNodeExists(curator, newNode);
        assertNotEquals(originalNode, newNode);
    }

    @Test
    public void testRecreatesNodeWhenItGetsDeleted() throws Exception
    {
        CuratorFramework curator = newCurator();

        PersistentEphemeralNode node = new PersistentEphemeralNode(curator, PersistentEphemeralNode.Mode.PROTECTED_EPHEMERAL, PATH, new byte[0]);
        node.start();
        node.waitForInitialCreate(10, TimeUnit.SECONDS);
        String originalNode = node.getActualPath();
        assertNodeExists(curator, originalNode);

        // Delete the original node...
        curator.delete().forPath(originalNode);

        // Since we're using an ephemeral node, and the original session hasn't been interrupted the name of the new
        // node that gets created is going to be exactly the same as the original.
        Trigger createdWatchTrigger = Trigger.created();
        Stat stat = curator.checkExists().usingWatcher(createdWatchTrigger).forPath(originalNode);
        assertTrue(stat != null || createdWatchTrigger.firedWithin(10, TimeUnit.SECONDS));
    }

    @Test
    public void testNodesCreateUniquePaths() throws Exception
    {
        CuratorFramework curator = newCurator();

        PersistentEphemeralNode node1 = new PersistentEphemeralNode(curator, PersistentEphemeralNode.Mode.PROTECTED_EPHEMERAL, PATH, new byte[0]);
        node1.start();
        node1.waitForInitialCreate(10, TimeUnit.SECONDS);
        String path1 = node1.getActualPath();

        PersistentEphemeralNode node2 = new PersistentEphemeralNode(curator, PersistentEphemeralNode.Mode.PROTECTED_EPHEMERAL, PATH, new byte[0]);
        node2.start();
        node2.waitForInitialCreate(10, TimeUnit.SECONDS);
        String path2 = node2.getActualPath();

        assertFalse(path1.equals(path2));
    }

    @Test
    public void testData() throws Exception
    {
        CuratorFramework curator = newCurator();
        byte[] data = "Hello World".getBytes();

        PersistentEphemeralNode node = new PersistentEphemeralNode(curator, PersistentEphemeralNode.Mode.PROTECTED_EPHEMERAL, PATH, data);
        node.start();
        node.waitForInitialCreate(10, TimeUnit.SECONDS);
        assertTrue(Arrays.equals(curator.getData().forPath(node.getActualPath()), data));
    }

    private void assertNodeExists(CuratorFramework curator, String path) throws Exception
    {
        assertNotNull(path);
        assertTrue(curator.checkExists().forPath(path) != null, path);
    }

    private void assertNodeDoesNotExist(CuratorFramework curator, String path) throws Exception
    {
        assertTrue(curator.checkExists().forPath(path) == null);
    }

    private CuratorFramework newCurator() throws IOException
    {
        Timing timing = new Timing();
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        client.start();

        clients.add(client);
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