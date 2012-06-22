package com.netflix.curator.framework.recipes.locks;

import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.recipes.BaseClassForTests;
import com.netflix.curator.retry.RetryOneTime;
import com.netflix.curator.test.Timing;
import junit.framework.Assert;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.testng.annotations.Test;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TestReaper extends BaseClassForTests
{
    @Test
    public void testRemove() throws Exception
    {
        Timing                  timing = new Timing();
        Reaper                  reaper = null;
        CuratorFramework        client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        try
        {
            client.start();
            client.create().creatingParentsIfNeeded().forPath("/one/two/three");

            Assert.assertNotNull(client.checkExists().forPath("/one/two/three"));

            reaper = new Reaper(client, 100);
            reaper.start();

            reaper.addPath("/one/two/three");
            timing.sleepABit();

            Assert.assertNull(client.checkExists().forPath("/one/two/three"));

            Assert.assertTrue(reaper.removePath("/one/two/three"));

            client.create().forPath("/one/two/three");
            timing.sleepABit();
            Assert.assertNotNull(client.checkExists().forPath("/one/two/three"));
        }
        finally
        {
            Closeables.closeQuietly(reaper);
            Closeables.closeQuietly(client);
        }
    }

    @Test
    public void testSimulationWithLocks() throws Exception
    {
        final int           LOCK_CLIENTS = 10;
        final int           ITERATIONS = 250;
        final int           MAX_WAIT_MS = 10;

        ExecutorService                     service = Executors.newFixedThreadPool(LOCK_CLIENTS);
        ExecutorCompletionService<Object>   completionService = new ExecutorCompletionService<Object>(service);

        Timing                      timing = new Timing();
        Reaper                      reaper = null;
        final CuratorFramework      client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        try
        {
            client.start();

            reaper = new Reaper(client, MAX_WAIT_MS / 2);
            reaper.start();
            reaper.addPath("/a/b");

            for ( int i = 0; i < LOCK_CLIENTS; ++i )
            {
                completionService.submit
                (
                    new Callable<Object>()
                    {
                        @Override
                        public Object call() throws Exception
                        {
                            final InterProcessMutex     lock = new InterProcessMutex(client, "/a/b");
                            for ( int i = 0; i < ITERATIONS; ++i )
                            {
                                lock.acquire();
                                try
                                {
                                    Thread.sleep((int)(Math.random() * MAX_WAIT_MS));
                                }
                                finally
                                {
                                    lock.release();
                                }
                            }
                            return null;
                        }
                    }
                );
            }

            for ( int i = 0; i < LOCK_CLIENTS; ++i )
            {
                completionService.take().get();
            }

            Thread.sleep(timing.session());
            timing.sleepABit();

            Stat stat = client.checkExists().forPath("/a/b");
            Assert.assertNull("Child qty: " + ((stat != null) ? stat.getNumChildren() : 0), stat);
        }
        finally
        {
            service.shutdownNow();
            Closeables.closeQuietly(reaper);
            Closeables.closeQuietly(client);
        }
    }

    @Test
    public void testWithEphemerals() throws Exception
    {
        Timing                  timing = new Timing();
        Reaper                  reaper = null;
        CuratorFramework        client2 = null;
        CuratorFramework        client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        try
        {
            client.start();
            client.create().creatingParentsIfNeeded().forPath("/one/two/three");

            Assert.assertNotNull(client.checkExists().forPath("/one/two/three"));

            client2 = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
            client2.start();
            for ( int i = 0; i < 10; ++i )
            {
                client2.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath("/one/two/three/foo-");
            }

            reaper = new Reaper(client, 100);
            reaper.start();

            reaper.addPath("/one/two/three");
            timing.sleepABit();

            Assert.assertNotNull(client.checkExists().forPath("/one/two/three"));

            client2.close();    // should clear ephemerals
            client2 = null;

            Thread.sleep(timing.session());
            timing.sleepABit();

            Assert.assertNull(client.checkExists().forPath("/one/two/three"));
        }
        finally
        {
            Closeables.closeQuietly(reaper);
            Closeables.closeQuietly(client2);
            Closeables.closeQuietly(client);
        }
    }

    @Test
    public void testBasic() throws Exception
    {
        Timing                  timing = new Timing();
        Reaper                  reaper = null;
        CuratorFramework        client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        try
        {
            client.start();
            client.create().creatingParentsIfNeeded().forPath("/one/two/three");

            Assert.assertNotNull(client.checkExists().forPath("/one/two/three"));

            reaper = new Reaper(client, 100);
            reaper.start();

            reaper.addPath("/one/two/three");
            timing.sleepABit();

            Assert.assertNull(client.checkExists().forPath("/one/two/three"));
        }
        finally
        {
            Closeables.closeQuietly(reaper);
            Closeables.closeQuietly(client);
        }
    }
}
