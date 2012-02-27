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
import com.netflix.curator.test.KillSession;
import com.netflix.curator.test.TestingServer;
import com.netflix.curator.test.Timing;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.io.File;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

public class BasicTests extends BaseClassForTests
{
    @Test
    public void     testExpiredSession() throws Exception
    {
        // see http://wiki.apache.org/hadoop/ZooKeeper/FAQ#A4

        final Timing                  timing = new Timing();

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

        final CuratorZookeeperClient client = new CuratorZookeeperClient(server.getConnectString(), timing.session(), timing.connection(), watcher, new RetryOneTime(2));
        client.start();
        try
        {
            RetryLoop.callWithRetry
            (
                client,
                new Callable<Object>()
                {
                    @Override
                    public Object call() throws Exception
                    {
                        client.getZooKeeper().create("/foo", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

                        KillSession.kill(client.getZooKeeper(), server.getConnectString());

                        Assert.assertTrue(timing.awaitLatch(latch));
                        ZooKeeper zooKeeper = client.getZooKeeper();
                        client.blockUntilConnectedOrTimedOut();
                        Assert.assertNotNull(zooKeeper.exists("/foo", false));
                        return null;
                    }
                }
            );
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
            client.blockUntilConnectedOrTimedOut();

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
            client.blockUntilConnectedOrTimedOut();
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
        final int CONNECTION_TIMEOUT_MS = 4000;

        CuratorZookeeperClient client = new CuratorZookeeperClient(server.getConnectString(), 10000, CONNECTION_TIMEOUT_MS, null, new RetryOneTime(1));
        client.start();
        try
        {
            Assert.assertFalse(client.isConnected());

            outer: do
            {
                for ( int i = 0; i < (CONNECTION_TIMEOUT_MS / 1000); ++i )
                {
                    if ( client.isConnected() )
                    {
                        break outer;
                    }

                    Thread.sleep(CONNECTION_TIMEOUT_MS);
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
