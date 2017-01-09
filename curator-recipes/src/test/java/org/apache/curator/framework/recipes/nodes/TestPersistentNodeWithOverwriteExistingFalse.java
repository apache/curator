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

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

public class TestPersistentNodeWithOverwriteExistingFalse extends BaseClassForTests
{
    @Test
    public void testQuickSetData() throws Exception
    {
        final byte[] TEST_DATA = "hey".getBytes();
        final byte[] ALT_TEST_DATA = "there".getBytes();

        Timing timing = new Timing();
        PersistentNode pen = null;
        CuratorFramework client = CuratorFrameworkFactory
                .newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        try
        {
            client.start();
            pen = new PersistentNode(client, CreateMode.PERSISTENT, false, false, "/test", TEST_DATA);
            pen.start();
            try
            {
                pen.setData(ALT_TEST_DATA);
                Assert.fail("IllegalStateException should have been thrown");
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

        Timing timing = new Timing();
        PersistentNode pen = null;
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        try
        {
            client.start();
            pen = new PersistentNode(client, CreateMode.PERSISTENT, false, false, "/test", TEST_DATA);
            pen.start();
            Assert.assertTrue(pen.waitForInitialCreate(timing.milliseconds(), TimeUnit.MILLISECONDS));
            client.close(); // cause session to end - force checks that node is persistent

            client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
            client.start();

            byte[] bytes = client.getData().forPath("/test");
            Assert.assertEquals(bytes, TEST_DATA);
        }
        finally
        {
            CloseableUtils.closeQuietly(pen);
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testClose() throws Exception
    {
        Timing timing = new Timing();
        PersistentNode pen = null;
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        try
        {
            client.start();
            pen = new PersistentNode(client, CreateMode.PERSISTENT, false, false, "/test/one/two", new byte[0]);
            pen.start();
            // we are only allowed to delete the node on close, once we know it belongs to us => wait for initial create
            Assert.assertTrue(pen.waitForInitialCreate(timing.milliseconds(), TimeUnit.MILLISECONDS));
            pen.close();
            timing.sleepABit();
            Assert.assertNull(client.checkExists().forPath("/test/one/two"));
        }
        finally
        {
            CloseableUtils.closeQuietly(pen);
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testDontOverwriteOrDeleteExisting() throws Exception
    {
        final byte[] TEST_DATA = "hey".getBytes();
        final byte[] ALT_TEST_DATA = "there".getBytes();

        Timing timing = new Timing();
        PersistentNode pen = null;
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        try
        {
            client.start();
            client.create().creatingParentsIfNeeded().forPath("/test/one/two", TEST_DATA);

            pen = new PersistentNode(client, CreateMode.PERSISTENT, false, false, "/test/one/two", ALT_TEST_DATA);
            pen.start();
            timing.sleepABit();
            Assert.assertEquals(client.getData().forPath("/test/one/two"), TEST_DATA);
            pen.close();
            timing.sleepABit();
            Assert.assertEquals(client.getData().forPath("/test/one/two"), TEST_DATA);
        }
        finally
        {
            CloseableUtils.closeQuietly(pen);
            CloseableUtils.closeQuietly(client);
        }
    }
}
