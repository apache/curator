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

import com.google.common.collect.Sets;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.Set;
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
    public void testSetAddition()
    {
        Watcher watcher = new Watcher()
        {
            @Override
            public void process(WatchedEvent event)
            {

            }
        };
        NamespaceWatcher namespaceWatcher1 = new NamespaceWatcher(null, watcher, "/foo");
        NamespaceWatcher namespaceWatcher2 = new NamespaceWatcher(null, watcher, "/foo");
        Assert.assertEquals(namespaceWatcher1, namespaceWatcher2);
        Assert.assertTrue(namespaceWatcher1.equals(watcher));
        Set<Watcher> set = Sets.newHashSet();
        set.add(namespaceWatcher1);
        set.add(namespaceWatcher2);
        Assert.assertEquals(set.size(), 1);
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
