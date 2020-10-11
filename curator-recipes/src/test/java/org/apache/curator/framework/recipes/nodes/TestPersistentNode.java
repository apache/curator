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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.Timing;
import org.apache.curator.test.compatibility.Timing2;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class TestPersistentNode extends BaseClassForTests
{
    @Test
    public void testQuickSetData() throws Exception
    {
        final byte[] TEST_DATA = "hey".getBytes();
        final byte[] ALT_TEST_DATA = "there".getBytes();

        Timing timing = new Timing();
        PersistentNode pen = null;
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        try
        {
            client.start();
            pen = new PersistentNode(client, CreateMode.PERSISTENT, false, "/test", TEST_DATA);
            pen.start();
            try
            {
                pen.setData(ALT_TEST_DATA);
                fail("IllegalStateException should have been thrown");
            }
            catch ( IllegalStateException dummy )
            {
                // expected
            }
        }
        finally
        {
            CloseableUtils.closeQuietly(pen);
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testBasic() throws Exception
    {
        final byte[] TEST_DATA = "hey".getBytes();

        Timing2 timing = new Timing2();
        PersistentNode pen = null;
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        try
        {
            client.start();
            pen = new PersistentNode(client, CreateMode.PERSISTENT, false, "/test", TEST_DATA);
            pen.debugWaitMsForBackgroundBeforeClose.set(timing.forSleepingABit().milliseconds());
            pen.start();
            assertTrue(pen.waitForInitialCreate(timing.milliseconds(), TimeUnit.MILLISECONDS));
            client.close(); // cause session to end - force checks that node is persistent

            client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
            client.start();

            byte[] bytes = client.getData().forPath("/test");
            assertArrayEquals(bytes, TEST_DATA);
        }
        finally
        {
            CloseableUtils.closeQuietly(pen);
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testQuickClose() throws Exception
    {
        Timing timing = new Timing();
        PersistentNode pen = null;
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        try
        {
            client.start();
            pen = new PersistentNode(client, CreateMode.PERSISTENT, false, "/test/one/two", new byte[0]);
            pen.start();
            pen.close();
            timing.sleepABit();
            assertNull(client.checkExists().forPath("/test/one/two"));
        }
        finally
        {
            CloseableUtils.closeQuietly(pen);
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testQuickCloseNodeExists() throws Exception
    {
        Timing timing = new Timing();
        PersistentNode pen = null;
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        try
        {
            client.start();
            client.create().creatingParentsIfNeeded().forPath("/test/one/two");

            pen = new PersistentNode(client, CreateMode.PERSISTENT, false, "/test/one/two", new byte[0]);
            pen.start();
            pen.close();
            timing.sleepABit();
            assertNull(client.checkExists().forPath("/test/one/two"));
        }
        finally
        {
            CloseableUtils.closeQuietly(pen);
            CloseableUtils.closeQuietly(client);
        }
    }
    
    @Test
    public void testEphemeralSequentialWithProtectionReconnection() throws Exception
    {
        Timing timing = new Timing();
        PersistentNode pen = null;
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        try
        {
            client.start();
            client.create().creatingParentsIfNeeded().forPath("/test/one");

            pen = new PersistentNode(client, CreateMode.EPHEMERAL_SEQUENTIAL, true, "/test/one/two", new byte[0]);
            pen.start();
            List<String> children = client.getChildren().forPath("/test/one");
            System.out.println("children before restart: "+children);
            assertEquals(1, children.size());
            server.stop();
            timing.sleepABit();
            server.restart();
            timing.sleepABit();
            List<String> childrenAfter = client.getChildren().forPath("/test/one");
            System.out.println("children after restart: "+childrenAfter);
            assertEquals(children, childrenAfter, "unexpected znodes: "+childrenAfter);
        }
        finally
        {
            CloseableUtils.closeQuietly(pen);
            CloseableUtils.closeQuietly(client);
        }
    }
}
