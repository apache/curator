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
package org.apache.curator.v2;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.compatibility.CuratorTestBase;
import org.apache.curator.v2.AsyncCuratorFrameworkV2;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.Watcher;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.concurrent.CountDownLatch;

public class TestAsyncCuratorFrameworkV2 extends CuratorTestBase
{
    @Test
    public void testPersistentRecursiveWatch() throws Exception
    {
        try ( CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1)) )
        {
            AsyncCuratorFrameworkV2 async = AsyncCuratorFrameworkV2.wrap(client);

            client.start();
            client.blockUntilConnected();

            CountDownLatch latch = new CountDownLatch(5);
            Watcher watcher = event -> latch.countDown();
            async.addWatch().withMode(AddWatchMode.PERSISTENT_RECURSIVE).usingWatcher(watcher).forPath("/test").toCompletableFuture().get();

            client.create().forPath("/test");
            client.create().forPath("/test/a");
            client.create().forPath("/test/a/b");
            client.create().forPath("/test/a/b/c");
            client.create().forPath("/test/a/b/c/d");

            Assert.assertTrue(timing.awaitLatch(latch));
        }
    }
}
