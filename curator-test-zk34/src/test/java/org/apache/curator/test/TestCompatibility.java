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
package org.apache.curator.test;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.compatibility.CuratorTestBase;
import org.apache.curator.utils.CloseableUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestCompatibility extends CuratorTestBase
{
    @Test
    public void testAutoState()
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        Assert.assertTrue(client.isZk34CompatibilityMode());
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testTtl() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            client.create().withTtl(100).forPath("/foo");
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testReconfig() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            client.reconfig().withNewMembers("a", "b");
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testGetConfig() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            client.getConfig().forEnsemble();
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testRemoveWatches() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            client.watches().removeAll();
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }
}
