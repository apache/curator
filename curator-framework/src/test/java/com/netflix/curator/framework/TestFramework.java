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
package com.netflix.curator.framework;

import com.netflix.curator.framework.api.BackgroundCallback;
import com.netflix.curator.framework.api.CuratorEvent;
import com.netflix.curator.framework.api.CuratorEventType;
import com.netflix.curator.framework.api.CuratorListener;
import com.netflix.curator.retry.RetryOneTime;
import com.netflix.curator.utils.EnsurePath;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class TestFramework extends BaseClassForTests
{
    @Test
    public void     testCreateACL() throws Exception
    {
        CuratorFrameworkFactory.Builder      builder = CuratorFrameworkFactory.builder();
        CuratorFramework client = builder.connectString(server.getConnectString()).authorization("digest", "me:pass".getBytes()).retryPolicy(new RetryOneTime(1)).build();
        client.start();
        try
        {
            client.create().withACL(ZooDefs.Ids.CREATOR_ALL_ACL).forPath("/test", new byte[0]);

            client.setData().forPath("/test", "test".getBytes());

            client.close();
            client = builder.connectString(server.getConnectString()).authorization("digest", "someone:else".getBytes()).retryPolicy(new RetryOneTime(1)).build();
            client.start();
            try
            {
                client.setData().forPath("/test", "test".getBytes());
                Assert.fail("Should be prohibited due to auth");
            }
            catch ( KeeperException.NoAuthException e )
            {
                // expected
            }
        }
        finally
        {
            client.close();
        }
    }

    @Test
    public void     testCreateParents() throws Exception
    {
        CuratorFrameworkFactory.Builder      builder = CuratorFrameworkFactory.builder();
        CuratorFramework client = builder.connectString(server.getConnectString()).retryPolicy(new RetryOneTime(1)).build();
        client.start();
        try
        {
            client.create().creatingParentsIfNeeded().forPath("/one/two/three", "foo".getBytes());
            byte[]      data = client.getData().forPath("/one/two/three");
            Assert.assertEquals(data, "foo".getBytes());

            client.create().creatingParentsIfNeeded().forPath("/one/two/another", "bar".getBytes());
            data = client.getData().forPath("/one/two/another");
            Assert.assertEquals(data, "bar".getBytes());
        }
        finally
        {
            client.close();
        }
    }

    @Test
    public void     testEnsurePathWithNamespace() throws Exception
    {
        final String namespace = "jz";

        CuratorFrameworkFactory.Builder      builder = CuratorFrameworkFactory.builder();
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
            client.close();
        }
    }

    @Test
    public void     testNamespace() throws Exception
    {
        final String namespace = "TestNamespace";
        
        CuratorFrameworkFactory.Builder      builder = CuratorFrameworkFactory.builder();
        CuratorFramework client = builder.connectString(server.getConnectString()).retryPolicy(new RetryOneTime(1)).namespace(namespace).build();
        client.start();
        try
        {
            String      actualPath = client.create().forPath("/test", new byte[0]);
            Assert.assertEquals(actualPath, "/test");
            Assert.assertNotNull(client.getZookeeperClient().getZooKeeper().exists("/" + namespace + "/test", false));
            Assert.assertNull(client.getZookeeperClient().getZooKeeper().exists("/test", false));

            actualPath = client.nonNamespaceView().create().forPath("/non", new byte[0]);
            Assert.assertEquals(actualPath, "/non");
            Assert.assertNotNull(client.getZookeeperClient().getZooKeeper().exists("/non", false));

            client.create().forPath("/test/child", "hey".getBytes());
            byte[]      bytes = client.getData().forPath("/test/child");
            Assert.assertEquals(bytes, "hey".getBytes());

            bytes = client.nonNamespaceView().getData().forPath("/" + namespace + "/test/child");
            Assert.assertEquals(bytes, "hey".getBytes());
        }
        finally
        {
            client.close();
        }
    }

    @Test
    public void     testCustomCallback() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            final CountDownLatch    latch = new CountDownLatch(1);
            BackgroundCallback      callback = new BackgroundCallback()
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
            client.create().inBackground(callback).forPath("/head", new byte[0]);
            Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
        }
        finally
        {
            client.close();
        }
    }

    @Test
    public void     testSync() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            client.addListener
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

                    @Override
                    public void clientClosedDueToError(CuratorFramework client, int resultCode, Throwable e)
                    {
                    }
                }
            );

            client.create().forPath("/head", new byte[0]);
            Assert.assertNotNull(client.checkExists().forPath("/head"));

            CountDownLatch      latch = new CountDownLatch(1);
            client.sync("/head", latch);
            Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
        }
        finally
        {
            client.close();
        }
    }

    @Test
    public void     testBackgroundDelete() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            client.addListener
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

                    @Override
                    public void clientClosedDueToError(CuratorFramework client, int resultCode, Throwable e)
                    {
                    }
                }
            );

            client.create().forPath("/head", new byte[0]);
            Assert.assertNotNull(client.checkExists().forPath("/head"));

            CountDownLatch      latch = new CountDownLatch(1);
            client.delete().inBackground(latch).forPath("/head");
            Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
            Assert.assertNull(client.checkExists().forPath("/head"));
        }
        finally
        {
            client.close();
        }
    }

    @Test
    public void     testDelete() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            client.create().forPath("/head", new byte[0]);
            Assert.assertNotNull(client.checkExists().forPath("/head"));
            client.delete().forPath("/head");
            Assert.assertNull(client.checkExists().forPath("/head"));
        }
        finally
        {
            client.close();
        }
    }

    @Test
    public void     testGetSequentialChildren() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            client.create().forPath("/head", new byte[0]);

            for ( int i = 0; i < 10; ++i )
            {
                client.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath("/head/child", new byte[0]);
            }

            List<String>        children = client.getChildren().forPath("/head");
            Assert.assertEquals(children.size(), 10);
        }
        finally
        {
            client.close();
        }
    }

    @Test
    public void     testBackgroundGetDataWithWatch() throws Exception
    {
        final byte[]        data1 = {1, 2, 3};
        final byte[]        data2 = {4, 5, 6, 7};

        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            final CountDownLatch          watchedLatch = new CountDownLatch(1);
            client.addListener
            (
                new CuratorListener()
                {
                    @Override
                    public void clientClosedDueToError(CuratorFramework client, int resultCode, Throwable e)
                    {
                    }

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

            CountDownLatch      backgroundLatch = new CountDownLatch(1);
            client.getData().watched().inBackground(backgroundLatch).forPath("/test");
            Assert.assertTrue(backgroundLatch.await(10, TimeUnit.SECONDS));

            client.setData().forPath("/test", data2);
            Assert.assertTrue(watchedLatch.await(10, TimeUnit.SECONDS));
            byte[]      checkData = client.getData().forPath("/test");
            Assert.assertEquals(checkData, data2);
        }
        finally
        {
            client.close();
        }
    }

    @Test
    public void     testBackgroundCreate() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            client.addListener
            (
                new CuratorListener()
                {
                    @Override
                    public void clientClosedDueToError(CuratorFramework client, int resultCode, Throwable e)
                    {
                    }

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

            CountDownLatch     latch = new CountDownLatch(1);
            client.create().inBackground(latch).forPath("/test", new byte[]{1, 2, 3});
            Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
        }
        finally
        {
            client.close();
        }
    }

    @Test
    public void     testCreateModes() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            byte[]  writtenBytes = {1, 2, 3};
            client.create().forPath("/test", writtenBytes); // should be persistent

            client.close();
            client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
            client.start();

            byte[]  readBytes = client.getData().forPath("/test");
            Assert.assertEquals(writtenBytes, readBytes);

            client.create().withMode(CreateMode.EPHEMERAL).forPath("/ghost", writtenBytes);

            client.close();
            client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
            client.start();

            readBytes = client.getData().forPath("/test");
            Assert.assertEquals(writtenBytes, readBytes);
            Stat    stat = client.checkExists().forPath("/ghost");
            Assert.assertNull(stat);

            String  realPath = client.create().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath("/pseq", writtenBytes);
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
            client.close();
        }
    }

    @Test
    public void     testSimple() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            String    path = client.create().withMode(CreateMode.PERSISTENT).forPath("/test", new byte[]{1, 2, 3});
            Assert.assertEquals(path, "/test");
        }
        finally
        {
            client.close();
        }
    }
}
