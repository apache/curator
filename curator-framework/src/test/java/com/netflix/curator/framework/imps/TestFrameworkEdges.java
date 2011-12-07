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
package com.netflix.curator.framework.imps;

import com.netflix.curator.RetryPolicy;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.api.CuratorEvent;
import com.netflix.curator.framework.api.CuratorEventType;
import com.netflix.curator.framework.api.CuratorListener;
import com.netflix.curator.framework.state.ConnectionState;
import com.netflix.curator.framework.state.ConnectionStateListener;
import com.netflix.curator.retry.RetryOneTime;
import com.netflix.curator.utils.TestingServer;
import com.netflix.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TestFrameworkEdges extends BaseClassForTests
{
    @Test
    public void     testMissedResponseOnESCreate() throws Exception
    {
        CuratorFramework                client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            CreateBuilderImpl               createBuilder = (CreateBuilderImpl)client.create();
            createBuilder.failNextCreateForTesting = true;
            String                          ourPath = createBuilder.withProtectedEphemeralSequential().forPath("/");
            Assert.assertTrue(ourPath.startsWith(ZKPaths.makePath("/", CreateBuilderImpl.PROTECTED_PREFIX)));
            Assert.assertFalse(createBuilder.failNextCreateForTesting);
        }
        finally
        {
            client.close();
        }
    }

    @Test
    public void     testSessionKilled() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            client.create().forPath("/sessionTest");

            final AtomicBoolean     sessionDied = new AtomicBoolean(false);
            Watcher         watcher = new Watcher()
            {
                @Override
                public void process(WatchedEvent event)
                {
                    if ( event.getState() == Event.KeeperState.Expired )
                    {
                        sessionDied.set(true);
                    }
                }
            };
            client.checkExists().usingWatcher(watcher).forPath("/sessionTest");
            KillSession.kill(server.getConnectString(), client.getZookeeperClient().getZooKeeper().getSessionId(), client.getZookeeperClient().getZooKeeper().getSessionPasswd());
            Assert.assertNotNull(client.checkExists().forPath("/sessionTest"));
            Assert.assertTrue(sessionDied.get());
        }
        finally
        {
            client.close();
        }
    }

    @Test
    public void         testNestedCalls() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            client.getCuratorListenable().addListener
            (
                new CuratorListener()
                {
                    @Override
                    public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception
                    {
                        if ( event.getType() == CuratorEventType.EXISTS )
                        {
                            Stat    stat = client.checkExists().forPath("/yo/yo/yo");
                            Assert.assertNull(stat);

                            client.create().inBackground(event.getContext()).forPath("/what");
                        }
                        else if ( event.getType() == CuratorEventType.CREATE )
                        {
                            ((CountDownLatch)event.getContext()).countDown();
                        }
                    }
                }
            );

            CountDownLatch        latch = new CountDownLatch(1);
            client.checkExists().inBackground(latch).forPath("/hey");
            Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
        }
        finally
        {
            client.close();
        }
    }

    @Test
    public void         testBackgroundFailure() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), 100, 100, new RetryOneTime(1));
        client.start();
        try
        {
            final CountDownLatch        latch = new CountDownLatch(1);
            client.getConnectionStateListenable().addListener
            (
                new ConnectionStateListener()
                {
                    @Override
                    public void stateChanged(CuratorFramework client, ConnectionState newState)
                    {
                        if ( newState == ConnectionState.LOST )
                        {
                            latch.countDown();
                        }
                    }
                }
            );

            client.checkExists().forPath("/hey");
            client.checkExists().inBackground().forPath("/hey");

            server.stop();

            client.checkExists().inBackground().forPath("/hey");
            Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
        }
        finally
        {
            client.close();
        }
    }

    @Test
    public void         testFailure() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), 100, 100, new RetryOneTime(1));
        client.start();
        try
        {
            client.checkExists().forPath("/hey");
            client.checkExists().inBackground().forPath("/hey");

            server.stop();

            client.checkExists().forPath("/hey");
            Assert.fail();
        }
        catch ( KeeperException.ConnectionLossException e )
        {
            // correct
        }
        finally
        {
            client.close();
        }
    }

    @Test
    public void         testRetry() throws Exception
    {
        final int       MAX_RETRIES = 3;
        final int       serverPort = server.getPort();

        final CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), 1000, 1000, new RetryOneTime(10));
        client.start();
        try
        {
/*
            client.getUnhandledErrorListenable().addListener
            (
                new UnhandledErrorListener()
                {
                    @Override
                    public void unhandledError(String message, Throwable e)
                    {
                    }
                }
            );
*/

            final AtomicInteger     retries = new AtomicInteger(0);
            final Semaphore         semaphore = new Semaphore(0);
            client.getZookeeperClient().setRetryPolicy
            (
                new RetryPolicy()
                {
                    @Override
                    public boolean allowRetry(int retryCount, long elapsedTimeMs)
                    {
                        semaphore.release();
                        if ( retries.incrementAndGet() == MAX_RETRIES )
                        {
                            try
                            {
                                server = new TestingServer(serverPort);
                            }
                            catch ( Exception e )
                            {
                                throw new Error(e);
                            }
                        }
                        return true;
                    }
                }
            );

            server.stop();

            // test foreground retry
            client.checkExists().forPath("/hey");
            Assert.assertTrue(semaphore.tryAcquire(MAX_RETRIES, 10, TimeUnit.SECONDS));

            semaphore.drainPermits();
            retries.set(0);

            server.stop();

            // test background retry
            client.checkExists().inBackground().forPath("/hey");
            Assert.assertTrue(semaphore.tryAcquire(MAX_RETRIES, 10, TimeUnit.SECONDS));
        }
        catch ( Throwable e )
        {
            Assert.fail("Error", e);
        }
        finally
        {
            client.close();
        }
    }

    @Test
    public void         testNotStarted() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.getData();
            Assert.fail();
        }
        catch ( Exception e )
        {
            // correct
        }
    }

    @Test
    public void         testStopped() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        client.getData();
        client.close();

        try
        {
            client.getData();
            Assert.fail();
        }
        catch ( Exception e )
        {
            // correct
        }
    }
}
