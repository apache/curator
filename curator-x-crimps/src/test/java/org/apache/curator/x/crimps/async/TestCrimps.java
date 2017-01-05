package org.apache.curator.x.crimps.async;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

public class TestCrimps extends BaseClassForTests
{
    @Test
    public void testCreateAndSet() throws Exception
    {
        try ( CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1)) )
        {
            client.start();
            CompletionStage<String> f = Crimps.nameInBackground(client.create().creatingParentContainersIfNeeded().withMode(CreateMode.PERSISTENT_SEQUENTIAL)).forPath("/a/b/c");
            complete(f.handle((path, e) -> {
                Assert.assertEquals(path, "/a/b/c0000000000");
                return null;
            }));

            f = Crimps.nameInBackground(client.create()).forPath("/foo/bar");
            assertException(f, KeeperException.Code.NONODE);

            CompletionStage<Stat> statFuture = Crimps.statBytesInBackground(client.setData()).forPath("/a/b/c0000000000", "hey".getBytes());
            complete(statFuture.handle((stat, e) -> {
                Assert.assertNotNull(stat);
                return null;
            }));
        }
    }

    @Test
    public void testDelete() throws Exception
    {
        try ( CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1)) )
        {
            client.start();
            client.create().forPath("/test");

            CompletionStage<Void> f = Crimps.voidInBackground(client.delete()).forPath("/test");
            complete(f.handle((v, e) -> {
                Assert.assertEquals(v, null);
                Assert.assertEquals(e, null);
                return null;
            }));

            f = Crimps.voidInBackground(client.delete()).forPath("/test");
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

            CompletionStage<byte[]> f = Crimps.dataInBackground(client.getData()).forPath("/test");
            complete(f.handle((data, e) -> {
                Assert.assertEquals(data, "foo".getBytes());
                return null;
            }));
        }
    }

    public void assertException(CompletionStage<?> f, KeeperException.Code code) throws Exception
    {
        complete(f.handle((value, e) -> {
            if ( e == null )
            {
                Assert.fail(code + " expected");
            }
            KeeperException keeperException = unwrap(e);
            Assert.assertNotNull(keeperException);
            Assert.assertEquals(keeperException.code(), code);
            return null;
        }));
    }

    private void complete(CompletionStage<?> f) throws Exception
    {
        try
        {
            f.toCompletableFuture().get();
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
        }
        catch ( ExecutionException e )
        {
            if ( e.getCause() instanceof AssertionError )
            {
                throw ((AssertionError)e.getCause());
            }
            throw e;
        }
    }

    private static KeeperException unwrap(Throwable e)
    {
        while ( e != null )
        {
            if ( e instanceof KeeperException )
            {
                return (KeeperException)e;
            }
            e = e.getCause();
        }
        return null;
    }
}
