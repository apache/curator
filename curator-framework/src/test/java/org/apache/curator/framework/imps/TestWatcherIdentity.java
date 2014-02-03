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

import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.Timing;
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
            CloseableUtils.closeQuietly(client);
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
            CloseableUtils.closeQuietly(client);
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
            CloseableUtils.closeQuietly(client);
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
            CloseableUtils.closeQuietly(client);
        }
    }
}
