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

import com.google.common.collect.Lists;
import org.apache.curator.framework.AuthInfo;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.Timing;
import org.apache.curator.test.compatibility.Timing2;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.EnsurePath;
import org.apache.curator.utils.ZKPaths;
import org.apache.curator.utils.ZookeeperFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("deprecation")
public class TestFramework extends BaseClassForTests
{
    @BeforeMethod
    @Override
    public void setup() throws Exception
    {
        System.setProperty("znode.container.checkIntervalMs", "1000");
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
    public void testWaitForShutdownTimeoutMs() throws Exception
    {
        final BlockingQueue<Integer> timeoutQueue = new ArrayBlockingQueue<>(1);
        ZookeeperFactory zookeeperFactory = new ZookeeperFactory()
        {
            @Override
            public ZooKeeper newZooKeeper(String connectString, int sessionTimeout, Watcher watcher, boolean canBeReadOnly) throws IOException
            {
                return new ZooKeeper(connectString, sessionTimeout, watcher, canBeReadOnly)
                {
                    @Override
                    public boolean close(int waitForShutdownTimeoutMs) throws InterruptedException
                    {
                        timeoutQueue.add(waitForShutdownTimeoutMs);
                        return super.close(waitForShutdownTimeoutMs);
                    }
                };
            }
        };

        CuratorFramework client = CuratorFrameworkFactory.builder()
            .connectString(server.getConnectString())
            .retryPolicy(new RetryOneTime(1))
            .zookeeperFactory(zookeeperFactory)
            .waitForShutdownTimeoutMs(10064)
            .build();
        try
        {
            client.start();
            client.checkExists().forPath("/foo");
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }

        Integer polledValue = timeoutQueue.poll(new Timing().milliseconds(), TimeUnit.MILLISECONDS);
        Assert.assertNotNull(polledValue);
        Assert.assertEquals(10064, polledValue.intValue());
    }
    
    @Test
    public void testSessionLossWithLongTimeout() throws Exception
    {
        final Timing timing = new Timing();

        try(final CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.forWaiting().milliseconds(),
                                                                              timing.connection(), new RetryOneTime(1)))
        {
            final CountDownLatch connectedLatch = new CountDownLatch(1);
            final CountDownLatch lostLatch = new CountDownLatch(1);
            final CountDownLatch restartedLatch = new CountDownLatch(1);
            client.getConnectionStateListenable().addListener(new ConnectionStateListener()
            {
                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState)
                {
                    if ( newState == ConnectionState.CONNECTED )
                    {
                        connectedLatch.countDown();
                    }
                    else if ( newState == ConnectionState.LOST )
                    {
                        lostLatch.countDown();
                    }
                    else if ( newState == ConnectionState.RECONNECTED  )
                    {
                        restartedLatch.countDown();
                    }
                }
            });
            
            client.start();
            
            Assert.assertTrue(timing.awaitLatch(connectedLatch));
            
            server.stop();

            timing.sleepABit();
            Assert.assertTrue(timing.awaitLatch(lostLatch));
            
            server.restart();
            Assert.assertTrue(timing.awaitLatch(restartedLatch));
        }
    }

    @Test
    public void testConnectionState() throws Exception
    {
        Timing timing = new Timing();
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        try
        {
            final BlockingQueue<ConnectionState> queue = new LinkedBlockingQueue<ConnectionState>();
            ConnectionStateListener listener = new ConnectionStateListener()
            {
                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState)
                {
                    queue.add(newState);
                }
            };
            client.getConnectionStateListenable().addListener(listener);

            client.start();
            Assert.assertEquals(queue.poll(timing.multiple(4).seconds(), TimeUnit.SECONDS), ConnectionState.CONNECTED);
            server.stop();
            Assert.assertEquals(queue.poll(timing.multiple(4).seconds(), TimeUnit.SECONDS), ConnectionState.SUSPENDED);
            Assert.assertEquals(queue.poll(timing.multiple(4).seconds(), TimeUnit.SECONDS), ConnectionState.LOST);
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testCreateOrSetData() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();

            String name = client.create().forPath("/hey", "there".getBytes());
            Assert.assertEquals(name, "/hey");
            name = client.create().orSetData().forPath("/hey", "other".getBytes());
            Assert.assertEquals(name, "/hey");
            Assert.assertEquals(client.getData().forPath("/hey"), "other".getBytes());

            name = client.create().orSetData().creatingParentsIfNeeded().forPath("/a/b/c", "there".getBytes());
            Assert.assertEquals(name, "/a/b/c");
            name = client.create().orSetData().creatingParentsIfNeeded().forPath("/a/b/c", "what".getBytes());
            Assert.assertEquals(name, "/a/b/c");
            Assert.assertEquals(client.getData().forPath("/a/b/c"), "what".getBytes());

            final BlockingQueue<CuratorEvent> queue = new LinkedBlockingQueue<>();
            BackgroundCallback backgroundCallback = new BackgroundCallback()
            {
                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
                {
                    queue.add(event);
                }
            };
            client.create().orSetData().inBackground(backgroundCallback).forPath("/a/b/c", "another".getBytes());

            CuratorEvent event = queue.poll(new Timing().milliseconds(), TimeUnit.MILLISECONDS);
            Assert.assertNotNull(event);
            Assert.assertEquals(event.getResultCode(), KeeperException.Code.OK.intValue());
            Assert.assertEquals(event.getType(), CuratorEventType.CREATE);
            Assert.assertEquals(event.getPath(), "/a/b/c");
            Assert.assertEquals(event.getName(), "/a/b/c");

            // callback should only be called once
            CuratorEvent unexpectedEvent = queue.poll(new Timing().milliseconds(), TimeUnit.MILLISECONDS);
            Assert.assertNull(unexpectedEvent);
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testCreateOrSetDataWithNoZkWatches() throws Exception
    {
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        CuratorFramework client = builder.connectString(server.getConnectString()).createZookeeperWatches(false).retryPolicy(new RetryOneTime(1)).build();
        try
        {
            client.start();

            String name = client.create().forPath("/hey", "there".getBytes());
            Assert.assertEquals(name, "/hey");
            name = client.create().orSetData().forPath("/hey", "other".getBytes());
            Assert.assertEquals(name, "/hey");
            Assert.assertEquals(client.getData().forPath("/hey"), "other".getBytes());

            name = client.create().orSetData().creatingParentsIfNeeded().forPath("/a/b/c", "there".getBytes());
            Assert.assertEquals(name, "/a/b/c");
            name = client.create().orSetData().creatingParentsIfNeeded().forPath("/a/b/c", "what".getBytes());
            Assert.assertEquals(name, "/a/b/c");
            Assert.assertEquals(client.getData().forPath("/a/b/c"), "what".getBytes());

            final BlockingQueue<CuratorEvent> queue = new LinkedBlockingQueue<>();
            BackgroundCallback backgroundCallback = new BackgroundCallback()
            {
                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
                {
                    queue.add(event);
                }
            };
            client.create().orSetData().inBackground(backgroundCallback).forPath("/a/b/c", "another".getBytes());

            CuratorEvent event = queue.poll(new Timing().milliseconds(), TimeUnit.MILLISECONDS);
            Assert.assertNotNull(event);
            Assert.assertEquals(event.getResultCode(), KeeperException.Code.OK.intValue());
            Assert.assertEquals(event.getType(), CuratorEventType.CREATE);
            Assert.assertEquals(event.getPath(), "/a/b/c");
            Assert.assertEquals(event.getName(), "/a/b/c");

            // callback should only be called once
            CuratorEvent unexpectedEvent = queue.poll(new Timing().milliseconds(), TimeUnit.MILLISECONDS);
            Assert.assertNull(unexpectedEvent);
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testQuietDelete() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();

            client.delete().quietly().forPath("/foo/bar");

            final BlockingQueue<Integer> rc = new LinkedBlockingQueue<>();
            BackgroundCallback backgroundCallback = new BackgroundCallback()
            {
                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
                {
                    rc.add(event.getResultCode());
                }
            };
            client.delete().quietly().inBackground(backgroundCallback).forPath("/foo/bar/hey");

            Integer code = rc.poll(new Timing().milliseconds(), TimeUnit.MILLISECONDS);
            Assert.assertNotNull(code);
            Assert.assertEquals(code.intValue(), KeeperException.Code.OK.intValue());
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testNamespaceWithWatcher() throws Exception
    {
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        CuratorFramework client = builder.connectString(server.getConnectString()).namespace("aisa").retryPolicy(new RetryOneTime(1)).build();
        client.start();
        try
        {
            final BlockingQueue<String> queue = new LinkedBlockingQueue<String>();
            Watcher watcher = new Watcher()
            {
                @Override
                public void process(WatchedEvent event)
                {
                    try
                    {
                        queue.put(event.getPath());
                    }
                    catch ( InterruptedException e )
                    {
                        throw new Error(e);
                    }
                }
            };
            client.create().forPath("/base");
            client.getChildren().usingWatcher(watcher).forPath("/base");
            client.create().forPath("/base/child");

            String path = new Timing2().takeFromQueue(queue);
            Assert.assertEquals(path, "/base");
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testNamespaceInBackground() throws Exception
    {
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        CuratorFramework client = builder.connectString(server.getConnectString()).namespace("aisa").retryPolicy(new RetryOneTime(1)).build();
        client.start();
        try
        {
            final BlockingQueue<String> queue = new LinkedBlockingQueue<String>();
            CuratorListener listener = new CuratorListener()
            {
                @Override
                public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception
                {
                    if ( event.getType() == CuratorEventType.EXISTS )
                    {
                        queue.put(event.getPath());
                    }
                }
            };

            client.getCuratorListenable().addListener(listener);
            client.create().forPath("/base");
            client.checkExists().inBackground().forPath("/base");

            String path = queue.poll(10, TimeUnit.SECONDS);
            Assert.assertEquals(path, "/base");

            client.getCuratorListenable().removeListener(listener);

            BackgroundCallback callback = new BackgroundCallback()
            {
                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
                {
                    queue.put(event.getPath());
                }
            };
            client.getChildren().inBackground(callback).forPath("/base");
            path = queue.poll(10, TimeUnit.SECONDS);
            Assert.assertEquals(path, "/base");
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testCreateACLSingleAuth() throws Exception
    {
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        CuratorFramework client = builder
            .connectString(server.getConnectString())
            .authorization("digest", "me1:pass1".getBytes())
            .retryPolicy(new RetryOneTime(1))
            .build();
        client.start();
        try
        {
            ACL acl = new ACL(ZooDefs.Perms.WRITE, ZooDefs.Ids.AUTH_IDS);
            List<ACL> aclList = Lists.newArrayList(acl);
            client.create().withACL(aclList).forPath("/test", "test".getBytes());
            client.close();

            // Try setting data with me1:pass1
            client = builder
                .connectString(server.getConnectString())
                .authorization("digest", "me1:pass1".getBytes())
                .retryPolicy(new RetryOneTime(1))
                .build();
            client.start();
            try
            {
                client.setData().forPath("/test", "test".getBytes());
            }
            catch ( KeeperException.NoAuthException e )
            {
                Assert.fail("Auth failed");
            }
            client.close();

            // Try setting data with something:else
            client = builder
                .connectString(server.getConnectString())
                .authorization("digest", "something:else".getBytes())
                .retryPolicy(new RetryOneTime(1))
                .build();
            client.start();
            try
            {
                client.setData().forPath("/test", "test".getBytes());
                Assert.fail("Should have failed with auth exception");
            }
            catch ( KeeperException.NoAuthException e )
            {
                // expected
            }
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testACLDeprecatedApis() throws Exception
    {
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder()
            .connectString(server.getConnectString())
            .retryPolicy(new RetryOneTime(1));
        Assert.assertNull(builder.getAuthScheme());
        Assert.assertNull(builder.getAuthValue());

        builder = builder.authorization("digest", "me1:pass1".getBytes());
        Assert.assertEquals(builder.getAuthScheme(), "digest");
        Assert.assertEquals(builder.getAuthValue(), "me1:pass1".getBytes());
    }

    @Test
    public void testCreateACLMultipleAuths() throws Exception
    {
        // Add a few authInfos
        List<AuthInfo> authInfos = new ArrayList<AuthInfo>();
        authInfos.add(new AuthInfo("digest", "me1:pass1".getBytes()));
        authInfos.add(new AuthInfo("digest", "me2:pass2".getBytes()));

        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        CuratorFramework client = builder
            .connectString(server.getConnectString())
            .authorization(authInfos)
            .retryPolicy(new RetryOneTime(1))
            .build();
        client.start();
        try
        {
            ACL acl = new ACL(ZooDefs.Perms.WRITE, ZooDefs.Ids.AUTH_IDS);
            List<ACL> aclList = Lists.newArrayList(acl);
            client.create().withACL(aclList).forPath("/test", "test".getBytes());
            client.close();

            // Try setting data with me1:pass1
            client = builder
                .connectString(server.getConnectString())
                .authorization("digest", "me1:pass1".getBytes())
                .retryPolicy(new RetryOneTime(1))
                .build();
            client.start();
            try
            {
                client.setData().forPath("/test", "test".getBytes());
            }
            catch ( KeeperException.NoAuthException e )
            {
                Assert.fail("Auth failed");
            }
            client.close();

            // Try setting data with me1:pass1
            client = builder
                .connectString(server.getConnectString())
                .authorization("digest", "me2:pass2".getBytes())
                .retryPolicy(new RetryOneTime(1))
                .build();
            client.start();
            try
            {
                client.setData().forPath("/test", "test".getBytes());
            }
            catch ( KeeperException.NoAuthException e )
            {
                Assert.fail("Auth failed");
            }
            client.close();

            // Try setting data with something:else
            client = builder
                .connectString(server.getConnectString())
                .authorization("digest", "something:else".getBytes())
                .retryPolicy(new RetryOneTime(1))
                .build();
            client.start();
            try
            {
                client.setData().forPath("/test", "test".getBytes());
                Assert.fail("Should have failed with auth exception");
            }
            catch ( KeeperException.NoAuthException e )
            {
                // expected
            }
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testCreateACLWithReset() throws Exception
    {
        Timing timing = new Timing();
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        CuratorFramework client = builder
            .connectString(server.getConnectString())
            .sessionTimeoutMs(timing.session())
            .connectionTimeoutMs(timing.connection())
            .authorization("digest", "me:pass".getBytes())
            .retryPolicy(new RetryOneTime(1))
            .build();
        client.start();
        try
        {
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

            ACL acl = new ACL(ZooDefs.Perms.WRITE, ZooDefs.Ids.AUTH_IDS);
            List<ACL> aclList = Lists.newArrayList(acl);
            client.create().withACL(aclList).forPath("/test", "test".getBytes());

            server.stop();
            Assert.assertTrue(timing.awaitLatch(lostLatch));
            try
            {
                client.checkExists().forPath("/");
                Assert.fail("Connection should be down");
            }
            catch ( KeeperException.ConnectionLossException e )
            {
                // expected
            }

            server.restart();
            try
            {
                client.setData().forPath("/test", "test".getBytes());
            }
            catch ( KeeperException.NoAuthException e )
            {
                Assert.fail("Auth failed");
            }
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testCreateParents() throws Exception
    {
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        CuratorFramework client = builder.connectString(server.getConnectString()).retryPolicy(new RetryOneTime(1)).build();
        client.start();
        try
        {
            client.create().creatingParentsIfNeeded().forPath("/one/two/three", "foo".getBytes());
            byte[] data = client.getData().forPath("/one/two/three");
            Assert.assertEquals(data, "foo".getBytes());

            client.create().creatingParentsIfNeeded().forPath("/one/two/another", "bar".getBytes());
            data = client.getData().forPath("/one/two/another");
            Assert.assertEquals(data, "bar".getBytes());
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testOverrideCreateParentContainers() throws Exception
    {
        if ( !checkForContainers() )
        {
            return;
        }

        CuratorFramework client = CuratorFrameworkFactory.builder()
            .connectString(server.getConnectString())
            .retryPolicy(new RetryOneTime(1))
            .dontUseContainerParents()
            .build();
        try
        {
            client.start();
            client.create().creatingParentContainersIfNeeded().forPath("/one/two/three", "foo".getBytes());
            byte[] data = client.getData().forPath("/one/two/three");
            Assert.assertEquals(data, "foo".getBytes());

            client.delete().forPath("/one/two/three");
            new Timing().sleepABit();

            Assert.assertNotNull(client.checkExists().forPath("/one/two"));
            new Timing().sleepABit();
            Assert.assertNotNull(client.checkExists().forPath("/one"));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testCreateParentContainers() throws Exception
    {
        if ( !checkForContainers() )
        {
            return;
        }

        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        CuratorFramework client = builder.connectString(server.getConnectString()).retryPolicy(new RetryOneTime(1)).build();
        try
        {
            client.start();
            client.create().creatingParentContainersIfNeeded().forPath("/one/two/three", "foo".getBytes());
            byte[] data = client.getData().forPath("/one/two/three");
            Assert.assertEquals(data, "foo".getBytes());

            client.delete().forPath("/one/two/three");
            new Timing().sleepABit();

            Assert.assertNull(client.checkExists().forPath("/one/two"));
            new Timing().sleepABit();
            Assert.assertNull(client.checkExists().forPath("/one"));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }



    private boolean checkForContainers()
    {
        if ( ZKPaths.getContainerCreateMode() == CreateMode.PERSISTENT )
        {
            System.out.println("Not using CreateMode.CONTAINER enabled version of ZooKeeper");
            return false;
        }
        return true;
    }

    @Test
    public void testCreatingParentsTheSame() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();

            Assert.assertNull(client.checkExists().forPath("/one/two"));
            client.create().creatingParentContainersIfNeeded().forPath("/one/two/three");
            Assert.assertNotNull(client.checkExists().forPath("/one/two"));

            client.delete().deletingChildrenIfNeeded().forPath("/one");
            Assert.assertNull(client.checkExists().forPath("/one"));

            Assert.assertNull(client.checkExists().forPath("/one/two"));
            client.checkExists().creatingParentContainersIfNeeded().forPath("/one/two/three");
            Assert.assertNotNull(client.checkExists().forPath("/one/two"));
            Assert.assertNull(client.checkExists().forPath("/one/two/three"));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testExistsCreatingParents() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();

            Assert.assertNull(client.checkExists().forPath("/one/two"));
            client.checkExists().creatingParentContainersIfNeeded().forPath("/one/two/three");
            Assert.assertNotNull(client.checkExists().forPath("/one/two"));
            Assert.assertNull(client.checkExists().forPath("/one/two/three"));
            Assert.assertNull(client.checkExists().creatingParentContainersIfNeeded().forPath("/one/two/three"));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testExistsCreatingParentsInBackground() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();

            Assert.assertNull(client.checkExists().forPath("/one/two"));

            final CountDownLatch latch = new CountDownLatch(1);
            BackgroundCallback callback = new BackgroundCallback()
            {
                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
                {
                    latch.countDown();
                }
            };
            client.checkExists().creatingParentContainersIfNeeded().inBackground(callback).forPath("/one/two/three");
            Assert.assertTrue(new Timing().awaitLatch(latch));
            Assert.assertNotNull(client.checkExists().forPath("/one/two"));
            Assert.assertNull(client.checkExists().forPath("/one/two/three"));
            Assert.assertNull(client.checkExists().creatingParentContainersIfNeeded().forPath("/one/two/three"));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testEnsurePathWithNamespace() throws Exception
    {
        final String namespace = "jz";

        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        CuratorFramework client = builder.connectString(server.getConnectString()).retryPolicy(new RetryOneTime(1)).namespace(namespace).build();
        client.start();
        try
        {
            EnsurePath ensurePath = new EnsurePath("/pity/the/fool");
            ensurePath.ensure(client.getZookeeperClient());
            Assert.assertNull(client.getZookeeperClient().getZooKeeper().exists("/jz/pity/the/fool", false));

            ensurePath = client.newNamespaceAwareEnsurePath("/pity/the/fool");
            ensurePath.ensure(client.getZookeeperClient());
            Assert.assertNotNull(client.getZookeeperClient().getZooKeeper().exists("/jz/pity/the/fool", false));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }
    
    @Test
    public void testCreateContainersWithNamespace() throws Exception
    {
        final String namespace = "container1";
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        CuratorFramework client = builder.connectString(server.getConnectString()).retryPolicy(new RetryOneTime(1)).namespace(namespace).build();
        try
        {
            client.start();
            String path = "/path1/path2";
            client.createContainers(path);
            Assert.assertNotNull(client.checkExists().forPath(path));
            Assert.assertNotNull(client.getZookeeperClient().getZooKeeper().exists("/" + namespace + path, false));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }
    
    
    @Test
    public void testCreateContainersUsingNamespace() throws Exception
    {
        final String namespace = "container2";
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        CuratorFramework client = builder.connectString(server.getConnectString()).retryPolicy(new RetryOneTime(1)).build();
        try
        {
            client.start();
            CuratorFramework nsClient = client.usingNamespace(namespace);
            String path = "/path1/path2";
            nsClient.createContainers(path);
            Assert.assertNotNull(nsClient.checkExists().forPath(path));
            Assert.assertNotNull(nsClient.getZookeeperClient().getZooKeeper().exists("/" + namespace + path, false));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testNamespace() throws Exception
    {
        final String namespace = "TestNamespace";

        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        CuratorFramework client = builder.connectString(server.getConnectString()).retryPolicy(new RetryOneTime(1)).namespace(namespace).build();
        client.start();
        try
        {
            String actualPath = client.create().forPath("/test");
            Assert.assertEquals(actualPath, "/test");
            Assert.assertNotNull(client.getZookeeperClient().getZooKeeper().exists("/" + namespace + "/test", false));
            Assert.assertNull(client.getZookeeperClient().getZooKeeper().exists("/test", false));

            actualPath = client.usingNamespace(null).create().forPath("/non");
            Assert.assertEquals(actualPath, "/non");
            Assert.assertNotNull(client.getZookeeperClient().getZooKeeper().exists("/non", false));

            client.create().forPath("/test/child", "hey".getBytes());
            byte[] bytes = client.getData().forPath("/test/child");
            Assert.assertEquals(bytes, "hey".getBytes());

            bytes = client.usingNamespace(null).getData().forPath("/" + namespace + "/test/child");
            Assert.assertEquals(bytes, "hey".getBytes());
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testCustomCallback() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            final CountDownLatch latch = new CountDownLatch(1);
            BackgroundCallback callback = new BackgroundCallback()
            {
                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
                {
                    if ( event.getType() == CuratorEventType.CREATE )
                    {
                        if ( event.getPath().equals("/head") )
                        {
                            latch.countDown();
                        }
                    }
                }
            };
            client.create().inBackground(callback).forPath("/head");
            Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testSync() throws Exception
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
                            if ( event.getType() == CuratorEventType.SYNC )
                            {
                                Assert.assertEquals(event.getPath(), "/head");
                                ((CountDownLatch)event.getContext()).countDown();
                            }
                        }
                    }
                );

            client.create().forPath("/head");
            Assert.assertNotNull(client.checkExists().forPath("/head"));

            CountDownLatch latch = new CountDownLatch(1);
            client.sync("/head", latch);
            Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testSyncNew() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            client.create().forPath("/head");
            Assert.assertNotNull(client.checkExists().forPath("/head"));

            final CountDownLatch latch = new CountDownLatch(1);
            BackgroundCallback callback = new BackgroundCallback()
            {
                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
                {
                    if ( event.getType() == CuratorEventType.SYNC )
                    {
                        latch.countDown();
                    }
                }
            };
            client.sync().inBackground(callback).forPath("/head");
            Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testBackgroundDelete() throws Exception
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
                            if ( event.getType() == CuratorEventType.DELETE )
                            {
                                Assert.assertEquals(event.getPath(), "/head");
                                ((CountDownLatch)event.getContext()).countDown();
                            }
                        }
                    }
                );

            client.create().forPath("/head");
            Assert.assertNotNull(client.checkExists().forPath("/head"));

            CountDownLatch latch = new CountDownLatch(1);
            client.delete().inBackground(latch).forPath("/head");
            Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
            Assert.assertNull(client.checkExists().forPath("/head"));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testBackgroundDeleteWithChildren() throws Exception
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
                            if ( event.getType() == CuratorEventType.DELETE )
                            {
                                Assert.assertEquals(event.getPath(), "/one/two");
                                ((CountDownLatch)event.getContext()).countDown();
                            }
                        }
                    }
                );

            client.create().creatingParentsIfNeeded().forPath("/one/two/three/four");
            Assert.assertNotNull(client.checkExists().forPath("/one/two/three/four"));

            CountDownLatch latch = new CountDownLatch(1);
            client.delete().deletingChildrenIfNeeded().inBackground(latch).forPath("/one/two");
            Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
            Assert.assertNull(client.checkExists().forPath("/one/two"));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testDelete() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            client.create().forPath("/head");
            Assert.assertNotNull(client.checkExists().forPath("/head"));
            client.delete().forPath("/head");
            Assert.assertNull(client.checkExists().forPath("/head"));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testDeleteWithChildren() throws Exception
    {
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        CuratorFramework client = builder.connectString(server.getConnectString()).retryPolicy(new RetryOneTime(1)).build();
        client.start();
        try
        {
            client.create().creatingParentsIfNeeded().forPath("/one/two/three/four/five/six", "foo".getBytes());
            client.delete().deletingChildrenIfNeeded().forPath("/one/two/three/four/five");
            Assert.assertNull(client.checkExists().forPath("/one/two/three/four/five"));
            client.delete().deletingChildrenIfNeeded().forPath("/one/two");
            Assert.assertNull(client.checkExists().forPath("/one/two"));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testDeleteGuaranteedWithChildren() throws Exception
    {
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        CuratorFramework client = builder.connectString(server.getConnectString()).retryPolicy(new RetryOneTime(1)).build();
        client.start();
        try
        {
            client.create().creatingParentsIfNeeded().forPath("/one/two/three/four/five/six", "foo".getBytes());
            client.delete().guaranteed().deletingChildrenIfNeeded().forPath("/one/two/three/four/five");
            Assert.assertNull(client.checkExists().forPath("/one/two/three/four/five"));
            client.delete().guaranteed().deletingChildrenIfNeeded().forPath("/one/two");
            Assert.assertNull(client.checkExists().forPath("/one/two"));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testGetSequentialChildren() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            client.create().forPath("/head");

            for ( int i = 0; i < 10; ++i )
            {
                client.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath("/head/child");
            }

            List<String> children = client.getChildren().forPath("/head");
            Assert.assertEquals(children.size(), 10);
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testBackgroundGetDataWithWatch() throws Exception
    {
        final byte[] data1 = {1, 2, 3};
        final byte[] data2 = {4, 5, 6, 7};

        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            final CountDownLatch watchedLatch = new CountDownLatch(1);
            client.getCuratorListenable().addListener
                (
                    new CuratorListener()
                    {
                        @Override
                        public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception
                        {
                            if ( event.getType() == CuratorEventType.GET_DATA )
                            {
                                Assert.assertEquals(event.getPath(), "/test");
                                Assert.assertEquals(event.getData(), data1);
                                ((CountDownLatch)event.getContext()).countDown();
                            }
                            else if ( event.getType() == CuratorEventType.WATCHED )
                            {
                                if ( event.getWatchedEvent().getType() == Watcher.Event.EventType.NodeDataChanged )
                                {
                                    Assert.assertEquals(event.getPath(), "/test");
                                    watchedLatch.countDown();
                                }
                            }
                        }
                    }
                );

            client.create().forPath("/test", data1);

            CountDownLatch backgroundLatch = new CountDownLatch(1);
            client.getData().watched().inBackground(backgroundLatch).forPath("/test");
            Assert.assertTrue(backgroundLatch.await(10, TimeUnit.SECONDS));

            client.setData().forPath("/test", data2);
            Assert.assertTrue(watchedLatch.await(10, TimeUnit.SECONDS));
            byte[] checkData = client.getData().forPath("/test");
            Assert.assertEquals(checkData, data2);
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testBackgroundCreate() throws Exception
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
                            if ( event.getType() == CuratorEventType.CREATE )
                            {
                                Assert.assertEquals(event.getPath(), "/test");
                                ((CountDownLatch)event.getContext()).countDown();
                            }
                        }
                    }
                );

            CountDownLatch latch = new CountDownLatch(1);
            client.create().inBackground(latch).forPath("/test", new byte[]{1, 2, 3});
            Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testCreateModes() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            byte[] writtenBytes = {1, 2, 3};
            client.create().forPath("/test", writtenBytes); // should be persistent

            client.close();
            client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
            client.start();

            byte[] readBytes = client.getData().forPath("/test");
            Assert.assertEquals(writtenBytes, readBytes);

            client.create().withMode(CreateMode.EPHEMERAL).forPath("/ghost", writtenBytes);

            client.close();
            client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
            client.start();

            readBytes = client.getData().forPath("/test");
            Assert.assertEquals(writtenBytes, readBytes);
            Stat stat = client.checkExists().forPath("/ghost");
            Assert.assertNull(stat);

            String realPath = client.create().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath("/pseq", writtenBytes);
            Assert.assertNotSame(realPath, "/pseq");

            client.close();
            client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
            client.start();

            readBytes = client.getData().forPath(realPath);
            Assert.assertEquals(writtenBytes, readBytes);

            realPath = client.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath("/eseq", writtenBytes);
            Assert.assertNotSame(realPath, "/eseq");

            client.close();
            client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
            client.start();

            stat = client.checkExists().forPath(realPath);
            Assert.assertNull(stat);
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testSimple() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            String path = client.create().withMode(CreateMode.PERSISTENT).forPath("/test", new byte[]{1, 2, 3});
            Assert.assertEquals(path, "/test");
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testSequentialWithTrailingSeparator() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            client.create().forPath("/test");
            //This should create a node in the form of "/test/00000001"
            String path = client.create().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath("/test/");
            Assert.assertTrue(path.startsWith("/test/"));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }
}
