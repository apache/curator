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

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.WatcherRemoveCuratorFramework;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.Timing;
import org.apache.curator.test.WatchersDebug;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class TestWatcherRemovalManager extends BaseClassForTests
{
    @Test
    public void testBasic() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();
            internalTryBasic(client);
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testBasicNamespace1() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();
            internalTryBasic(client.usingNamespace("foo"));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testBasicNamespace2() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.builder()
            .connectString(server.getConnectString())
            .retryPolicy(new RetryOneTime(1))
            .namespace("hey")
            .build();
        try
        {
            client.start();
            internalTryBasic(client);
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testBasicNamespace3() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.builder()
            .connectString(server.getConnectString())
            .retryPolicy(new RetryOneTime(1))
            .namespace("hey")
            .build();
        try
        {
            client.start();
            internalTryBasic(client.usingNamespace("lakjsf"));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testSameWatcher() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();

            WatcherRemovalFacade removerClient = (WatcherRemovalFacade)client.newWatcherRemoveCuratorFramework();

            Watcher watcher = new Watcher()
            {
                @Override
                public void process(WatchedEvent event)
                {
                    // NOP
                }
            };

            removerClient.getData().usingWatcher(watcher).forPath("/");
            Assert.assertEquals(removerClient.getRemovalManager().getEntries().size(), 1);
            removerClient.getData().usingWatcher(watcher).forPath("/");
            Assert.assertEquals(removerClient.getRemovalManager().getEntries().size(), 1);
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testTriggered() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();

            WatcherRemovalFacade removerClient = (WatcherRemovalFacade)client.newWatcherRemoveCuratorFramework();

            final CountDownLatch latch = new CountDownLatch(1);
            Watcher watcher = new Watcher()
            {
                @Override
                public void process(WatchedEvent event)
                {
                    if ( event.getType() == Event.EventType.NodeCreated )
                    {
                        latch.countDown();
                    }
                }
            };

            removerClient.checkExists().usingWatcher(watcher).forPath("/yo");
            Assert.assertEquals(removerClient.getRemovalManager().getEntries().size(), 1);
            removerClient.create().forPath("/yo");

            Assert.assertTrue(new Timing().awaitLatch(latch));

            Assert.assertEquals(removerClient.getRemovalManager().getEntries().size(), 0);
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    private void internalTryBasic(CuratorFramework client) throws Exception
    {
        WatcherRemoveCuratorFramework removerClient = client.newWatcherRemoveCuratorFramework();

        Watcher watcher = new Watcher()
        {
            @Override
            public void process(WatchedEvent event)
            {
                // NOP
            }
        };
        removerClient.checkExists().usingWatcher(watcher).forPath("/hey");

        List<String> existWatches = WatchersDebug.getExistWatches(client.getZookeeperClient().getZooKeeper());
        Assert.assertEquals(existWatches.size(), 1);

        removerClient.removeWatchers();

        new Timing().sleepABit();

        existWatches = WatchersDebug.getExistWatches(client.getZookeeperClient().getZooKeeper());
        Assert.assertEquals(existWatches.size(), 0);
    }
}
