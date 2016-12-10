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
package org.apache.curator.framework.recipes.nodes;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.ZKPaths;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode.BUILD_INITIAL_CACHE;

public class TestPersistentTtlNode extends BaseClassForTests
{
    private final Timing timing = new Timing();

    @BeforeMethod
    @Override
    public void setup() throws Exception
    {
        System.setProperty("znode.container.checkIntervalMs", "1");
        super.setup();
    }

    @AfterMethod
    @Override
    public void teardown() throws Exception
    {
        System.clearProperty("znode.container.checkIntervalMs");
        super.teardown();
    }

    @Test
    public void testBasic() throws Exception
    {
        try (CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1)))
        {
            client.start();

            try (PersistentTtlNode node = new PersistentTtlNode(client, "/test", 100, new byte[0]))
            {
                node.start();
                node.waitForInitialCreate(timing.session(), TimeUnit.MILLISECONDS);

                for ( int i = 0; i < 10; ++i )
                {
                    Thread.sleep(110);  // sleep a bit more than the TTL
                    Assert.assertNotNull(client.checkExists().forPath("/test"));
                }
            }
            Assert.assertNotNull(client.checkExists().forPath("/test"));

            timing.sleepABit();
            Assert.assertNull(client.checkExists().forPath("/test"));
        }
    }

    @Test
    public void testForcedDeleteOfTouchNode() throws Exception
    {
        try (CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1)))
        {
            client.start();

            try (PersistentTtlNode node = new PersistentTtlNode(client, "/test", 10, new byte[0]))
            {
                node.start();
                node.waitForInitialCreate(timing.session(), TimeUnit.MILLISECONDS);

                for ( int i = 0; i < 10; ++i )
                {
                    Thread.sleep(10);
                    client.delete().quietly().forPath(ZKPaths.makePath("test", PersistentTtlNode.DEFAULT_CHILD_NODE_NAME));
                }

                timing.sleepABit();
                Assert.assertNotNull(client.checkExists().forPath("/test"));
            }
        }
    }

    @Test
    public void testEventsOnParent() throws Exception
    {
        try (CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1)))
        {
            client.start();

            try (PersistentTtlNode node = new PersistentTtlNode(client, "/test", 10, new byte[0]))
            {
                try(PathChildrenCache cache = new PathChildrenCache(client, "/", true))
                {
                    final Semaphore changes = new Semaphore(0);
                    PathChildrenCacheListener listener = new PathChildrenCacheListener()
                    {
                        @Override
                        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
                        {
                            if ( (event.getType() == PathChildrenCacheEvent.Type.CHILD_UPDATED) && "/test".equals(event.getData().getPath()) )
                            {
                                changes.release();
                            }
                        }
                    };
                    cache.getListenable().addListener(listener);

                    node.start();
                    node.waitForInitialCreate(timing.session(), TimeUnit.MILLISECONDS);
                    cache.start(BUILD_INITIAL_CACHE);

                    Assert.assertEquals(changes.availablePermits(), 0);
                    timing.sleepABit();
                    Assert.assertEquals(changes.availablePermits(), 0);

                    client.setData().forPath("/test", "changed".getBytes());
                    Assert.assertTrue(timing.acquireSemaphore(changes));
                    timing.sleepABit();
                    Assert.assertEquals(changes.availablePermits(), 0);
                }
            }

            timing.sleepABit();

            Assert.assertNull(client.checkExists().forPath("/test"));
        }
    }
}
