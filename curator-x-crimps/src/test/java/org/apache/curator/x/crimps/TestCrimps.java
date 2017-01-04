package org.apache.curator.x.crimps;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class TestCrimps extends BaseClassForTests
{
    private final Crimps crimps = Crimps.newCrimps();

    @Test
    public void testCreateAndSet() throws Exception
    {
        try ( CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1)) )
        {
            client.start();
            CompletableFuture<String> f = crimps.nameInBackground(client.create().creatingParentContainersIfNeeded().withMode(CreateMode.PERSISTENT_SEQUENTIAL)).forPath("/a/b/c");
            String path = f.get();
            Assert.assertEquals(path, "/a/b/c0000000000");

            f = crimps.nameInBackground(client.create()).forPath("/foo/bar");
            assertException(f, KeeperException.Code.NONODE);

            CompletableFuture<Stat> statFuture = crimps.statBytesInBackground(client.setData()).forPath(path, "hey".getBytes());
            Stat stat = statFuture.get();
            Assert.assertNotNull(stat);
        }
    }

    @Test
    public void testDelete() throws Exception
    {
        try ( CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1)) )
        {
            client.start();
            client.create().forPath("/test");

            CompletableFuture<Void> f = crimps.voidInBackground(client.delete()).forPath("/test");
            Void result = f.get();
            Assert.assertEquals(result, null);

            f = crimps.voidInBackground(client.delete()).forPath("/test");
            assertException(f, KeeperException.Code.NONODE);
        }
    }

    @Test
    public void testGetData() throws Exception
    {
        try ( CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1)) )
        {
            client.start();
            client.create().forPath("/test", "foo".getBytes());

            CompletableFuture<byte[]> f = crimps.dataInBackground(client.getData()).forPath("/test");
            byte[] data = f.get();
            Assert.assertEquals(data, "foo".getBytes());
        }
    }

    public void assertException(CompletableFuture<?> f, KeeperException.Code code) throws InterruptedException
    {
        try
        {
            f.get();
            Assert.fail();
        }
        catch ( ExecutionException e )
        {
            KeeperException keeperException = CrimpException.unwrap(e);
            Assert.assertNotNull(keeperException);
            Assert.assertEquals(keeperException.code(), code);
        }
    }
}
