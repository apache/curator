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
package org.apache.curator.framework.recipes.watch;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TestCuratorCache extends BaseClassForTests
{
    private static final Timing timing = new Timing();

    @Test
    public void testPathOnlyEvents() throws Exception
    {
        CuratorCache cache = null;
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        try
        {
            client.start();

            final BlockingQueue<CacheEvent> events = new LinkedBlockingQueue<>();
            cache = CuratorCacheBuilder.builder(client, "/test").withCacheSelector(CacheSelectors.pathOnly()).build();
            cache.getListenable().addListener(new CacheListener()
            {
                @Override
                public void process(CacheEvent event, String path, CachedNode affectedNode)
                {
                    events.offer(event);
                }
            });
            cache.start();

            Assert.assertEquals(events.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS), CacheEvent.CACHE_REFRESHED);
            client.create().forPath("/test", "one".getBytes());
            Assert.assertEquals(events.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS), CacheEvent.NODE_CREATED);
            client.setData().forPath("/test", "two".getBytes());
            Assert.assertEquals(events.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS), CacheEvent.NODE_CHANGED);
            client.delete().forPath("/test");
            Assert.assertEquals(events.poll(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS), CacheEvent.NODE_DELETED);
        }
        finally
        {
            CloseableUtils.closeQuietly(cache);
            CloseableUtils.closeQuietly(client);
        }
    }
}
