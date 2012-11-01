package com.netflix.curator.framework.imps;

import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryOneTime;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestNamespaceFacade extends BaseClassForTests
{
    @Test
    public void     testGetNamespace() throws Exception
    {
        CuratorFramework    client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        CuratorFramework    client2 = CuratorFrameworkFactory.builder().namespace("snafu").retryPolicy(new RetryOneTime(1)).connectString("").build();
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
            Closeables.closeQuietly(client2);
            Closeables.closeQuietly(client);
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
            Closeables.closeQuietly(client);
        }
    }

    @Test
    public void     testCache() throws Exception
    {
        CuratorFramework    client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();

            Assert.assertEquals(client.usingNamespace("foo"), client.usingNamespace("foo"));
            Assert.assertNotSame(client.usingNamespace("foo"), client.usingNamespace("bar"));
        }
        finally
        {
            Closeables.closeQuietly(client);
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
        }
        finally
        {
            Closeables.closeQuietly(client);
        }
    }

    @Test
    public void     testIsStarted() throws Exception
    {
        CuratorFramework    client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        CuratorFramework    namespaced = client.usingNamespace(null);
        try
        {
            Assert.assertEquals(client.isStarted(), namespaced.isStarted());

            client.start();

            Assert.assertEquals(client.isStarted(), namespaced.isStarted());
        }
        finally
        {
            Closeables.closeQuietly(client);
        }
    }
}
