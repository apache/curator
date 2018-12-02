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
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.compatibility.CuratorTestBase;
import org.apache.curator.test.Timing;
import org.apache.curator.test.compatibility.Zk35MethodInterceptor;
import org.apache.zookeeper.CreateMode;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.util.concurrent.CountDownLatch;

@Test(groups = Zk35MethodInterceptor.zk35Group)
public class TestTtlNodes extends CuratorTestBase
{
    @BeforeClass
    public static void setUpClass() {
        System.setProperty("zookeeper.extendedTypesEnabled", "true");
    }
    
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
        super.teardown();
        System.clearProperty("znode.container.checkIntervalMs");
    }

    @Test
    public void testBasic() throws Exception
    {
        try ( CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1)) )
        {
            client.start();

            client.create().withTtl(10).creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT_WITH_TTL).forPath("/a/b/c");
            Thread.sleep(20);
            Assert.assertNull(client.checkExists().forPath("/a/b/c"));
        }
    }

    @Test
    public void testBasicInBackground() throws Exception
    {
        try ( CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1)) )
        {
            client.start();

            final CountDownLatch latch = new CountDownLatch(1);
            BackgroundCallback callback = new BackgroundCallback()
            {
                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
                {
                    latch.countDown();
                }
            };
            client.create().withTtl(10).creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT_WITH_TTL).inBackground(callback).forPath("/a/b/c");
            Assert.assertTrue(new Timing().awaitLatch(latch));
            Thread.sleep(20);
            Assert.assertNull(client.checkExists().forPath("/a/b/c"));
        }
    }
}
