package com.netflix.curator.framework.imps;

import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryOneTime;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestCompression extends BaseClassForTests
{
    @Test
    public void testSetData() throws Exception
    {
        final byte[]            data = "here's a string".getBytes();

        CuratorFramework        client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();

            client.create().creatingParentsIfNeeded().forPath("/a/b/c", data);
            Assert.assertEquals(data, client.getData().forPath("/a/b/c"));

            client.setData().compressed().forPath("/a/b/c", data);
            Assert.assertEquals(data.length, client.getData().decompressed().forPath("/a/b/c").length);
        }
        finally
        {
            Closeables.closeQuietly(client);
        }
    }

    @Test
    public void testSimple() throws Exception
    {
        final byte[]            data = "here's a string".getBytes();

        CuratorFramework        client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();

            client.create().compressed().creatingParentsIfNeeded().forPath("/a/b/c", data);

            Assert.assertNotEquals(data, client.getData().forPath("/a/b/c"));
            Assert.assertEquals(data.length, client.getData().decompressed().forPath("/a/b/c").length);
        }
        finally
        {
            Closeables.closeQuietly(client);
        }
    }
}
