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

import com.google.common.collect.Queues;
import org.apache.curator.RetryPolicy;
import org.apache.curator.RetrySleeper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.SafeIsTtlMode;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CreateBuilder;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.api.ErrorListenerPathAndBytesable;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryForever;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingCluster;
import org.apache.curator.test.TestingServer;
import org.apache.curator.test.compatibility.KillSession2;
import org.apache.curator.test.compatibility.Timing2;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TestFrameworkEdges extends BaseClassForTests
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Timing2 timing = new Timing2();

    @BeforeClass
    public static void setUpClass()
    {
        System.setProperty("zookeeper.extendedTypesEnabled", "true");
    }

    @Test
    public void testInjectSessionExpiration() throws Exception
    {
        try (CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1)))
        {
            client.start();

            CountDownLatch expiredLatch = new CountDownLatch(1);
            Watcher watcher = event -> {
                if ( event.getState() == Watcher.Event.KeeperState.Expired )
                {
                    expiredLatch.countDown();
                }
            };
            client.checkExists().usingWatcher(watcher).forPath("/foobar");
            KillSession2.kill(client.getZookeeperClient().getZooKeeper());
            Assert.assertTrue(timing.awaitLatch(expiredLatch));
        }
    }

    @Test
    public void testProtectionWithKilledSession() throws Exception
    {
        server.stop();  // not needed

        // see CURATOR-498
        // attempt to re-create the state described in the bug report: create a 3 Instance ensemble;
        // have Curator connect to only 1 one of those instances; set failNextCreateForTesting to
        // simulate protection mode searching; kill the connected server when this happens;
        // wait for session timeout to elapse and then restart the instance. In most cases
        // this will cause the scenario as Curator will send the session cancel and do protection mode
        // search around the same time. The protection mode search should return first as it can be resolved
        // by the Instance Curator is connected to but the session kill needs a quorum vote (it's a
        // transaction)

        try (TestingCluster cluster = new TestingCluster(3))
        {
            cluster.start();
            InstanceSpec instanceSpec0 = cluster.getServers().get(0).getInstanceSpec();

            CountDownLatch serverStoppedLatch = new CountDownLatch(1);
            RetryPolicy retryPolicy = new RetryForever(100)
            {
                @Override
                public boolean allowRetry(int retryCount, long elapsedTimeMs, RetrySleeper sleeper)
                {
                    if ( serverStoppedLatch.getCount() > 0 )
                    {
                        try
                        {
                            cluster.killServer(instanceSpec0);
                        }
                        catch ( Exception e )
                        {
                            // ignore
                        }
                        serverStoppedLatch.countDown();
                    }
                    return super.allowRetry(retryCount, elapsedTimeMs, sleeper);
                }
            };

            try (CuratorFramework client = CuratorFrameworkFactory.newClient(instanceSpec0.getConnectString(), timing.session(), timing.connection(), retryPolicy))
            {
                BlockingQueue<String> createdNode = new LinkedBlockingQueue<>();
                BackgroundCallback callback = (__, event) -> {
                    if ( event.getType() == CuratorEventType.CREATE )
                    {
                        createdNode.offer(event.getPath());
                    }
                };

                client.start();
                client.create().forPath("/test");

                ErrorListenerPathAndBytesable<String> builder = client.create().withProtection().withMode(CreateMode.EPHEMERAL).inBackground(callback);
                ((CreateBuilderImpl)builder).failNextCreateForTesting = true;

                builder.forPath("/test/hey");

                Assert.assertTrue(timing.awaitLatch(serverStoppedLatch));
                timing.forSessionSleep().sleep();   // wait for session to expire
                cluster.restartServer(instanceSpec0);

                String path = timing.takeFromQueue(createdNode);
                List<String> children = client.getChildren().forPath("/test");
                Assert.assertEquals(Collections.singletonList(ZKPaths.getNodeFromPath(path)), children);
            }
        }
    }

    @Test
    public void testBackgroundLatencyUnSleep() throws Exception
    {
        server.stop();

        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();
            ((CuratorFrameworkImpl)client).sleepAndQueueOperationSeconds = Integer.MAX_VALUE;

            final CountDownLatch latch = new CountDownLatch(3);
            BackgroundCallback callback = new BackgroundCallback()
            {
                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
                {
                    if ( (event.getType() == CuratorEventType.CREATE) && (event.getResultCode() == KeeperException.Code.OK.intValue()) )
                    {
                        latch.countDown();
                    }
                }
            };
            // queue multiple operations for a more complete test
            client.create().inBackground(callback).forPath("/test");
            client.create().inBackground(callback).forPath("/test/one");
            client.create().inBackground(callback).forPath("/test/two");
            server.restart();

            Assert.assertTrue(timing.awaitLatch(latch));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testCreateContainersForBadConnect() throws Exception
    {
        final int serverPort = server.getPort();
        server.close();

        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), 1000, 1000, new RetryNTimes(10, timing.forSleepingABit().milliseconds()));
        try
        {
            new Thread()
            {
                @Override
                public void run()
                {
                    try
                    {
                        Thread.sleep(3000);
                        server = new TestingServer(serverPort, true);
                    }
                    catch ( Exception e )
                    {
                        e.printStackTrace();
                    }
                }
            }.start();

            client.start();
            client.createContainers("/this/does/not/exist");
            Assert.assertNotNull(client.checkExists().forPath("/this/does/not/exist"));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testQuickClose() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), 1, new RetryNTimes(0, 0));
        try
        {
            client.start();
            client.close();
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testProtectedCreateNodeDeletion() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), 1, new RetryNTimes(0, 0));
        try
        {
            client.start();

            for ( int i = 0; i < 2; ++i )
            {
                CuratorFramework localClient = (i == 0) ? client : client.usingNamespace("nm");
                localClient.create().forPath("/parent");
                Assert.assertEquals(localClient.getChildren().forPath("/parent").size(), 0);

                CreateBuilderImpl createBuilder = (CreateBuilderImpl)localClient.create();
                createBuilder.failNextCreateForTesting = true;
                FindAndDeleteProtectedNodeInBackground.debugInsertError.set(true);
                try
                {
                    createBuilder.withProtection().forPath("/parent/test");
                    Assert.fail("failNextCreateForTesting should have caused a ConnectionLossException");
                }
                catch ( KeeperException.ConnectionLossException e )
                {
                    // ignore, correct
                }

                timing.sleepABit();
                List<String> children = localClient.getChildren().forPath("/parent");
                Assert.assertEquals(children.size(), 0, children.toString()); // protected mode should have deleted the node

                localClient.delete().forPath("/parent");
            }
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testPathsFromProtectingInBackground() throws Exception
    {
        for ( CreateMode mode : CreateMode.values() )
        {
            internalTestPathsFromProtectingInBackground(mode);
        }
    }

    private void internalTestPathsFromProtectingInBackground(CreateMode mode) throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), 1, new RetryOneTime(1));
        try
        {
            client.start();

            client.create().creatingParentsIfNeeded().forPath("/a/b/c");

            final BlockingQueue<String> paths = new ArrayBlockingQueue<String>(2);
            BackgroundCallback callback = new BackgroundCallback()
            {
                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
                {
                    paths.put(event.getName());
                    paths.put(event.getPath());
                }
            };
            final String TEST_PATH = "/a/b/c/test-";
            long ttl = timing.forWaiting().milliseconds() * 1000;
            CreateBuilder firstCreateBuilder = client.create();
            if ( SafeIsTtlMode.isTtl(mode) )
            {
                firstCreateBuilder.withTtl(ttl);
            }
            firstCreateBuilder.withMode(mode).inBackground(callback).forPath(TEST_PATH);

            String name1 = timing.takeFromQueue(paths);
            String path1 = timing.takeFromQueue(paths);

            client.close();

            client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), 1, new RetryOneTime(1));
            client.start();

            CreateBuilderImpl createBuilder = (CreateBuilderImpl)client.create();
            createBuilder.withProtection();
            if ( SafeIsTtlMode.isTtl(mode) )
            {
                createBuilder.withTtl(ttl);
            }

            client.create().forPath(createBuilder.adjustPath(TEST_PATH));

            createBuilder.debugForceFindProtectedNode = true;
            createBuilder.withMode(mode).inBackground(callback).forPath(TEST_PATH);

            String name2 = timing.takeFromQueue(paths);
            String path2 = timing.takeFromQueue(paths);

            Assert.assertEquals(ZKPaths.getPathAndNode(name1).getPath(), ZKPaths.getPathAndNode(TEST_PATH).getPath());
            Assert.assertEquals(ZKPaths.getPathAndNode(name2).getPath(), ZKPaths.getPathAndNode(TEST_PATH).getPath());
            Assert.assertEquals(ZKPaths.getPathAndNode(path1).getPath(), ZKPaths.getPathAndNode(TEST_PATH).getPath());
            Assert.assertEquals(ZKPaths.getPathAndNode(path2).getPath(), ZKPaths.getPathAndNode(TEST_PATH).getPath());

            client.delete().deletingChildrenIfNeeded().forPath("/a/b/c");
            client.delete().forPath("/a/b");
            client.delete().forPath("/a");
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void connectionLossWithBackgroundTest() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), 1, new RetryOneTime(1));
        try
        {
            final CountDownLatch latch = new CountDownLatch(1);
            client.start();
            client.getZookeeperClient().blockUntilConnectedOrTimedOut();
            server.close();
            client.getChildren().inBackground(new BackgroundCallback()
            {
                public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
                {
                    latch.countDown();
                }
            }).forPath("/");
            Assert.assertTrue(timing.awaitLatch(latch));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testReconnectAfterLoss() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        try
        {
            client.start();

            final CountDownLatch lostLatch = new CountDownLatch(1);
            ConnectionStateListener listener = new ConnectionStateListener()
            {
                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState)
                {
                    if ( newState == ConnectionState.LOST )
                    {
                        lostLatch.countDown();
                    }
                }
            };
            client.getConnectionStateListenable().addListener(listener);

            client.checkExists().forPath("/");

            server.stop();

            Assert.assertTrue(timing.awaitLatch(lostLatch));

            try
            {
                client.checkExists().forPath("/");
                Assert.fail();
            }
            catch ( KeeperException.ConnectionLossException e )
            {
                // correct
            }

            server.restart();
            client.checkExists().forPath("/");
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testGetAclNoStat() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        client.start();
        try
        {
            try
            {
                client.getACL().forPath("/");
            }
            catch ( NullPointerException e )
            {
                Assert.fail();
            }
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testMissedResponseOnBackgroundESCreate() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        client.start();
        try
        {
            CreateBuilderImpl createBuilder = (CreateBuilderImpl)client.create();
            createBuilder.failNextCreateForTesting = true;

            final BlockingQueue<String> queue = Queues.newArrayBlockingQueue(1);
            BackgroundCallback callback = new BackgroundCallback()
            {
                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
                {
                    queue.put(event.getPath());
                }
            };
            createBuilder.withProtection().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).inBackground(callback).forPath("/");
            String ourPath = queue.poll(timing.forWaiting().seconds(), TimeUnit.SECONDS);
            Assert.assertTrue(ourPath.startsWith(ZKPaths.makePath("/", CreateBuilderImpl.PROTECTED_PREFIX)));
            Assert.assertFalse(createBuilder.failNextCreateForTesting);
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testMissedResponseOnESCreate() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        client.start();
        try
        {
            CreateBuilderImpl createBuilder = (CreateBuilderImpl)client.create();
            createBuilder.failNextCreateForTesting = true;
            String ourPath = createBuilder.withProtection().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath("/");
            Assert.assertTrue(ourPath.startsWith(ZKPaths.makePath("/", CreateBuilderImpl.PROTECTED_PREFIX)));
            Assert.assertFalse(createBuilder.failNextCreateForTesting);
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testSessionKilled() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        client.start();
        try
        {
            client.create().forPath("/sessionTest");

            final AtomicBoolean sessionDied = new AtomicBoolean(false);
            Watcher watcher = new Watcher()
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
            KillSession2.kill(client.getZookeeperClient().getZooKeeper());
            Assert.assertNotNull(client.checkExists().forPath("/sessionTest"));
            Assert.assertTrue(sessionDied.get());
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testNestedCalls() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        client.start();
        try
        {
            client.getCuratorListenable().addListener(new CuratorListener()
            {
                @Override
                public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception
                {
                    if ( event.getType() == CuratorEventType.EXISTS )
                    {
                        Stat stat = client.checkExists().forPath("/yo/yo/yo");
                        Assert.assertNull(stat);

                        client.create().inBackground(event.getContext()).forPath("/what");
                    }
                    else if ( event.getType() == CuratorEventType.CREATE )
                    {
                        ((CountDownLatch)event.getContext()).countDown();
                    }
                }
            });

            CountDownLatch latch = new CountDownLatch(1);
            client.checkExists().inBackground(latch).forPath("/hey");
            Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testBackgroundFailure() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        client.start();
        try
        {
            final CountDownLatch latch = new CountDownLatch(1);
            client.getConnectionStateListenable().addListener(new ConnectionStateListener()
            {
                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState)
                {
                    if ( newState == ConnectionState.LOST )
                    {
                        latch.countDown();
                    }
                }
            });

            client.checkExists().forPath("/hey");
            client.checkExists().inBackground().forPath("/hey");

            server.stop();

            client.checkExists().inBackground().forPath("/hey");
            Assert.assertTrue(timing.awaitLatch(latch));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testFailure() throws Exception
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
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testRetry() throws Exception
    {
        final int MAX_RETRIES = 3;

        final CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(10));
        client.start();
        try
        {
            final AtomicInteger retries = new AtomicInteger(0);
            final Semaphore semaphore = new Semaphore(0);
            RetryPolicy policy = new RetryPolicy()
            {
                @Override
                public boolean allowRetry(int retryCount, long elapsedTimeMs, RetrySleeper sleeper)
                {
                    semaphore.release();
                    if ( retries.incrementAndGet() == MAX_RETRIES )
                    {
                        try
                        {
                            server.restart();
                        }
                        catch ( Exception e )
                        {
                            throw new Error(e);
                        }
                    }
                    try
                    {
                        sleeper.sleepFor(100, TimeUnit.MILLISECONDS);
                    }
                    catch ( InterruptedException e )
                    {
                        Thread.currentThread().interrupt();
                    }
                    return true;
                }
            };
            client.getZookeeperClient().setRetryPolicy(policy);

            server.stop();

            // test foreground retry
            client.checkExists().forPath("/hey");
            Assert.assertTrue(semaphore.tryAcquire(MAX_RETRIES, timing.forWaiting().seconds(), TimeUnit.SECONDS), "Remaining leases: " + semaphore.availablePermits());

            // make sure we're reconnected
            client.getZookeeperClient().setRetryPolicy(new RetryOneTime(100));
            client.checkExists().forPath("/hey");

            client.getZookeeperClient().setRetryPolicy(policy);
            semaphore.drainPermits();
            retries.set(0);

            server.stop();

            // test background retry
            client.checkExists().inBackground().forPath("/hey");
            Assert.assertTrue(semaphore.tryAcquire(MAX_RETRIES, timing.forWaiting().seconds(), TimeUnit.SECONDS), "Remaining leases: " + semaphore.availablePermits());
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testNotStarted() throws Exception
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
        catch ( Throwable e )
        {
            Assert.fail("", e);
        }
    }

    @Test
    public void testStopped() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();
            client.getData();
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }

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
    public void testDeleteChildrenConcurrently() throws Exception
    {
        final CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        CuratorFramework client2 = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();
            client.getZookeeperClient().blockUntilConnectedOrTimedOut();
            client2.start();
            client2.getZookeeperClient().blockUntilConnectedOrTimedOut();

            int childCount = 5000;
            for ( int i = 0; i < childCount; i++ )
            {
                client.create().creatingParentsIfNeeded().forPath("/parent/child" + i);
            }

            final CountDownLatch latch = new CountDownLatch(1);
            new Thread(new Runnable()
            {
                @Override
                public void run()
                {
                    long start = System.currentTimeMillis();
                    try
                    {
                        client.delete().deletingChildrenIfNeeded().forPath("/parent");
                    }
                    catch ( Exception e )
                    {
                        if ( e instanceof KeeperException.NoNodeException )
                        {
                            Assert.fail("client delete failed, shouldn't throw NoNodeException", e);
                        }
                        else
                        {
                            Assert.fail("unexpected exception", e);
                        }
                    }
                    finally
                    {
                        log.info("client has deleted children, it costs: {}ms", System.currentTimeMillis() - start);
                        latch.countDown();
                    }
                }
            }).start();

            boolean threadDeleted = false;
            boolean client2Deleted = false;
            Random random = new Random();
            for ( int i = 0; i < childCount; i++ )
            {
                String child = "/parent/child" + random.nextInt(childCount);
                try
                {
                    if ( !threadDeleted )
                    {
                        Stat stat = client2.checkExists().forPath(child);
                        if ( stat == null )
                        {
                            // the thread client has begin deleted the children
                            threadDeleted = true;
                            log.info("client has deleted the child {}", child);
                        }
                    }
                    else
                    {
                        try
                        {
                            client2.delete().forPath(child);
                            client2Deleted = true;
                            log.info("client2 deleted the child {} successfully", child);
                            break;
                        }
                        catch ( Exception e )
                        {
                            if ( e instanceof KeeperException.NoNodeException )
                            {
                                // ignore, because it's deleted by the thread client
                            }
                            else
                            {
                                Assert.fail("unexpected exception", e);
                            }
                        }
                    }
                }
                catch ( Exception e )
                {
                    Assert.fail("unexpected exception", e);
                }
            }

            // The case run successfully, if client2 deleted a child successfully and the client deleted children successfully
            Assert.assertTrue(client2Deleted);
            Assert.assertTrue(timing.awaitLatch(latch));
            Assert.assertNull(client2.checkExists().forPath("/parent"));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
            CloseableUtils.closeQuietly(client2);
        }
    }
}
