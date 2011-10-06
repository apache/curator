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

import com.netflix.curator.retry.RetryOneTime;
import com.netflix.curator.utils.TestingServer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class BasicTests extends BaseClassForTests
{
    @Test
    public void     testExpiredSession() throws Exception
    {
        // see http://wiki.apache.org/hadoop/ZooKeeper/FAQ#A4

        final CountDownLatch    latch = new CountDownLatch(1);
        Watcher                 watcher = new Watcher()
        {
            @Override
            public void process(WatchedEvent event)
            {
                if ( event.getState() == Event.KeeperState.Expired )
                {
                    latch.countDown();
                }
            }
        };
        final CuratorZookeeperClient client = new CuratorZookeeperClient(server.getConnectString(), 10000, 10000, watcher, new RetryOneTime(2));
        client.start();
        try
        {
            client.blockUntilConnectedOrTimedOut();
            client.getZooKeeper().create("/foo", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

            final CountDownLatch    zkLatch = new CountDownLatch(1);
            Watcher         zkWatcher = new Watcher()
            {
                @Override
                public void process(WatchedEvent event)
                {
                    zkLatch.countDown();
                }
            };
            ZooKeeper       zk = new ZooKeeper(server.getConnectString(), 10000, zkWatcher, client.getZooKeeper().getSessionId(), client.getZooKeeper().getSessionPasswd());
            Assert.assertTrue(zkLatch.await(10, TimeUnit.SECONDS));
            zk.close(); // this should cause a session error in the main client

            Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
            Assert.assertNotNull(client.getZooKeeper().exists("/foo", false));
        }
        finally
        {
            client.close();
        }
    }

    @Test
    public void     testReconnect() throws Exception
    {
        int                 serverPort = server.getPort();
        File                tempDirectory = server.getTempDirectory();

        CuratorZookeeperClient client = new CuratorZookeeperClient(server.getConnectString(), 10000, 10000, null, new RetryOneTime(1));
        client.start();
        try
        {
            byte[]      writtenData = {1, 2, 3};
            client.getZooKeeper().create("/test", writtenData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            Thread.sleep(1000);
            server.stop();
            Thread.sleep(1000);

            server = new TestingServer(serverPort, tempDirectory);
            Assert.assertTrue(client.blockUntilConnectedOrTimedOut());
            byte[]      readData = client.getZooKeeper().getData("/test", false, null);
            Assert.assertEquals(readData, writtenData);
        }
        finally
        {
            client.close();
        }
    }

    @Test
    public void     testSimple() throws Exception
    {
        CuratorZookeeperClient client = new CuratorZookeeperClient(server.getConnectString(), 10000, 10000, null, new RetryOneTime(1));
        client.start();
        try
        {
            String              path = client.getZooKeeper().create("/test", new byte[]{1,2,3}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            Assert.assertEquals(path, "/test");
        }
        finally
        {
            client.close();
        }
    }

    @Test
    public void     testBackgroundConnect() throws Exception
    {
        CuratorZookeeperClient client = new CuratorZookeeperClient(server.getConnectString(), 10000, 4000, null, new RetryOneTime(1));
        client.start();
        try
        {
            Assert.assertFalse(client.isConnected());

            outer: do
            {
                for ( int i = 0; i < 5; ++i )
                {
                    if ( client.isConnected() )
                    {
                        break outer;
                    }

                    Thread.sleep(1000);
                }

                Assert.fail();
            } while ( false );
        }
        finally
        {
            client.close();
        }
    }
}
