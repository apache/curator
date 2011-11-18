/*
 *
 *  Copyright 2011 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package com.netflix.curator;

import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.netflix.curator.utils.TestingCluster;
import com.netflix.curator.utils.TestingServer;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class KillSession
{
    @Test
    public void     foo() throws Exception
    {
        TestingCluster      cluster = new TestingCluster(3);
        try
        {
            cluster.start();

            final CountDownLatch    latch = new CountDownLatch(1);
            Watcher                 watcher = new Watcher()
            {
                @Override
                public void process(WatchedEvent event)
                {
                    latch.countDown();
                    System.out.println(event);
                }
            };

            new ZooKeeper(cluster.getConnectString(), 1000, watcher);

            latch.await();
            Thread.sleep(5000);

            cluster.close();

            Thread.currentThread().join();
        }
        finally
        {
            //cluster.close();
        }
    }

    static void     kill(String connectString, long sessionId, byte[] sessionPassword) throws Exception
    {
        final CountDownLatch zkLatch = new CountDownLatch(1);
        Watcher zkWatcher = new Watcher()
        {
            @Override
            public void process(WatchedEvent event)
            {
                zkLatch.countDown();
            }
        };
        ZooKeeper zk = new ZooKeeper(connectString, 10000, zkWatcher, sessionId, sessionPassword);
        try
        {
            Assert.assertTrue(zkLatch.await(10, TimeUnit.SECONDS));
        }
        finally
        {
            zk.close(); // this should cause a session error in the main client
        }
    }
}
