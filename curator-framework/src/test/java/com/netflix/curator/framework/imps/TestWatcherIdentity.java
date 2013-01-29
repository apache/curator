package com.netflix.curator.framework.imps;

import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.api.CuratorWatcher;
import com.netflix.curator.retry.RetryOneTime;
import com.netflix.curator.test.Timing;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class TestWatcherIdentity extends BaseClassForTests
{
    private static final String PATH = "/foo";

    private static class CountCuratorWatcher implements CuratorWatcher
    {
        private final AtomicInteger count = new AtomicInteger(0);

        @Override
        public void process(WatchedEvent event) throws Exception
        {
            count.incrementAndGet();
        }
    }

    private static class CountZKWatcher implements Watcher
    {
        private final AtomicInteger count = new AtomicInteger(0);

        @Override
        public void process(WatchedEvent event)
        {
            System.out.println("count=" + count);
            count.incrementAndGet();
        }
    }

    @Test
    public void testRefExpiration() throws Exception
    {
        final int MAX_CHECKS = 10;

        final CuratorFrameworkImpl client = (CuratorFrameworkImpl)CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            Assert.assertNull(client.getNamespaceWatcherMap().get(new CountCuratorWatcher()));

            final CountDownLatch latch = new CountDownLatch(1);
            ExecutorService service = Executors.newSingleThreadExecutor();
            service.submit
            (
                new Callable<Void>()
                {
                    @Override
                    public Void call() throws Exception
                    {
                        CountZKWatcher watcher = new CountZKWatcher();
                        client.getNamespaceWatcherMap().getNamespaceWatcher(watcher);
                        Assert.assertNotNull(client.getNamespaceWatcherMap().get(watcher));
                        latch.countDown();
                        return null;
                    }
                }
            );

            latch.await();
            service.shutdownNow();

            Timing timing = new Timing();
            for ( int i = 0; i < MAX_CHECKS; ++i )
            {
                Assert.assertTrue((i + 1) < MAX_CHECKS);
                timing.sleepABit();

                client.getNamespaceWatcherMap().drain();  // just to cause drainReferenceQueues() to get called
                if ( client.getNamespaceWatcherMap().isEmpty() )
                {
                    break;
                }
            }
        }
        finally
        {
            Closeables.closeQuietly(client);
        }
    }

    @Test
    public void testSimpleId()
    {
        CountCuratorWatcher curatorWatcher = new CountCuratorWatcher();
        CountZKWatcher zkWatcher = new CountZKWatcher();
        CuratorFrameworkImpl client = (CuratorFrameworkImpl)CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            Assert.assertSame(client.getNamespaceWatcherMap().getNamespaceWatcher(curatorWatcher), client.getNamespaceWatcherMap().getNamespaceWatcher(curatorWatcher));
            Assert.assertSame(client.getNamespaceWatcherMap().getNamespaceWatcher(zkWatcher), client.getNamespaceWatcherMap().getNamespaceWatcher(zkWatcher));
            Assert.assertNotSame(client.getNamespaceWatcherMap().getNamespaceWatcher(curatorWatcher), client.getNamespaceWatcherMap().getNamespaceWatcher(zkWatcher));
        }
        finally
        {
            Closeables.closeQuietly(client);
        }
    }

    @Test
    public void testCuratorWatcher() throws Exception
    {
        Timing timing = new Timing();
        CountCuratorWatcher watcher = new CountCuratorWatcher();
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        try
        {
            client.start();
            client.create().forPath(PATH);
            // Add twice the same watcher on the same path
            client.getData().usingWatcher(watcher).forPath(PATH);
            client.getData().usingWatcher(watcher).forPath(PATH);
            // Ok, let's test it
            client.setData().forPath(PATH, new byte[]{});
            timing.sleepABit();
            Assert.assertEquals(1, watcher.count.get());
        }
        finally
        {
            Closeables.closeQuietly(client);
        }
    }


    @Test
    public void testZKWatcher() throws Exception
    {
        Timing timing = new Timing();
        CountZKWatcher watcher = new CountZKWatcher();
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        try
        {
            client.start();
            client.create().forPath(PATH);
            // Add twice the same watcher on the same path
            client.getData().usingWatcher(watcher).forPath(PATH);
            client.getData().usingWatcher(watcher).forPath(PATH);
            // Ok, let's test it
            client.setData().forPath(PATH, new byte[]{});
            timing.sleepABit();
            Assert.assertEquals(1, watcher.count.get());
        }
        finally
        {
            Closeables.closeQuietly(client);
        }
    }
}
