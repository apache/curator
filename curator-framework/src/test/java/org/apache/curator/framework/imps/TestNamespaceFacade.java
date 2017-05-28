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
package org.apache.curator.framework.imps;

import java.util.Collections;
import java.util.List;

import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryOneTime;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.KeeperException.NoAuthException;
import org.apache.zookeeper.data.ACL;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestNamespaceFacade extends BaseClassForTests
{
    @Test
    public void     testInvalid() throws Exception
    {
        try
        {
            CuratorFrameworkFactory.builder().namespace("/snafu").retryPolicy(new RetryOneTime(1)).connectString("").build();
            Assert.fail();
        }
        catch ( IllegalArgumentException e )
        {
            // correct
        }
    }

    @Test
    public void     testGetNamespace() throws Exception
    {
        CuratorFramework    client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        CuratorFramework    client2 = CuratorFrameworkFactory.builder().namespace("snafu").retryPolicy(new RetryOneTime(1)).connectString(server.getConnectString()).build();
        try
        {
            client.start();

            CuratorFramework fooClient = client.usingNamespace("foo");
            CuratorFramework barClient = client.usingNamespace("bar");

            Assert.assertEquals(client.getNamespace(), "");
            Assert.assertEquals(client2.getNamespace(), "snafu");
            Assert.assertEquals(fooClient.getNamespace(), "foo");
            Assert.assertEquals(barClient.getNamespace(), "bar");
        }
        finally
        {
            CloseableUtils.closeQuietly(client2);
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void     testSimultaneous() throws Exception
    {
        CuratorFramework    client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();

            CuratorFramework fooClient = client.usingNamespace("foo");
            CuratorFramework barClient = client.usingNamespace("bar");

            fooClient.create().forPath("/one");
            barClient.create().forPath("/one");

            Assert.assertNotNull(client.getZookeeperClient().getZooKeeper().exists("/foo/one", false));
            Assert.assertNotNull(client.getZookeeperClient().getZooKeeper().exists("/bar/one", false));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void     testCache() throws Exception
    {
        CuratorFramework    client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();

            Assert.assertSame(client.usingNamespace("foo"), client.usingNamespace("foo"));
            Assert.assertNotSame(client.usingNamespace("foo"), client.usingNamespace("bar"));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void     testBasic() throws Exception
    {
        CuratorFramework    client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();

            client.create().forPath("/one");
            Assert.assertNotNull(client.getZookeeperClient().getZooKeeper().exists("/one", false));

            client.usingNamespace("space").create().forPath("/one");
            Assert.assertNotNull(client.getZookeeperClient().getZooKeeper().exists("/space", false));

            client.usingNamespace("name").create().forPath("/one");
            Assert.assertNotNull(client.getZookeeperClient().getZooKeeper().exists("/name", false));
            Assert.assertNotNull(client.getZookeeperClient().getZooKeeper().exists("/name/one", false));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    /**
     * CURATOR-128: access root node within a namespace.
     */
    @Test
    public void     testRootAccess() throws Exception
    {
        CuratorFramework    client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();

            client.create().forPath("/one");
            Assert.assertNotNull(client.getZookeeperClient().getZooKeeper().exists("/one", false));

            Assert.assertNotNull(client.checkExists().forPath("/"));
            try
            {
                client.checkExists().forPath("");
                Assert.fail("IllegalArgumentException expected");
            }
            catch ( IllegalArgumentException expected )
            {
            }

            Assert.assertNotNull(client.usingNamespace("one").checkExists().forPath("/"));
            try
            {
                client.usingNamespace("one").checkExists().forPath("");
                Assert.fail("IllegalArgumentException expected");
            }
            catch ( IllegalArgumentException expected )
            {
            }
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }


    @Test
    public void     testIsStarted() throws Exception
    {
        CuratorFramework    client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        CuratorFramework    namespaced = client.usingNamespace(null);

        Assert.assertEquals(client.getState(), namespaced.getState(), "Namespaced state did not match true state after call to start.");

        client.close();
        Assert.assertEquals(client.getState(), namespaced.getState(), "Namespaced state did not match true state after call to close.");
    }
    
    /**
     * Test that ACLs work on a NamespaceFacade. See CURATOR-132
     * @throws Exception
     */
    @Test
    public void testACL() throws Exception
    {
        CuratorFramework    client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        client.getZookeeperClient().blockUntilConnectedOrTimedOut();

        client.create().creatingParentsIfNeeded().forPath("/parent/child", "A string".getBytes());
        CuratorFramework client2 = client.usingNamespace("parent");

        Assert.assertNotNull(client2.getData().forPath("/child"));  
        client.setACL().withACL(Collections.singletonList(
            new ACL(ZooDefs.Perms.WRITE, ZooDefs.Ids.ANYONE_ID_UNSAFE))).
                forPath("/parent/child");
        // This will attempt to setACL on /parent/child, Previously this failed because /child
        // isn't present. Using "child" would case a failure because the path didn't start with
        // a slash
        try
        {
            List<ACL> acls = client2.getACL().forPath("/child");
            Assert.assertNotNull(acls);
            Assert.assertEquals(acls.size(), 1);
            Assert.assertEquals(acls.get(0).getId(), ZooDefs.Ids.ANYONE_ID_UNSAFE);
            Assert.assertEquals(acls.get(0).getPerms(), ZooDefs.Perms.WRITE);
            client2.setACL().withACL(Collections.singletonList(
                new ACL(ZooDefs.Perms.DELETE, ZooDefs.Ids.ANYONE_ID_UNSAFE))).
                    forPath("/child");
            Assert.fail("Expected auth exception was not thrown");
        }
        catch(NoAuthException e)
        {
            //Expected
        }
    }

    @Test
    public void testUnfixForEmptyNamespace() {
        CuratorFramework client = CuratorFrameworkFactory.builder().namespace("").retryPolicy(new RetryOneTime(1)).connectString(server.getConnectString()).build();
        CuratorFrameworkImpl clientImpl = (CuratorFrameworkImpl) client;

        Assert.assertEquals(clientImpl.unfixForNamespace("/foo/bar"), "/foo/bar");

        CloseableUtils.closeQuietly(client);
    }
}
