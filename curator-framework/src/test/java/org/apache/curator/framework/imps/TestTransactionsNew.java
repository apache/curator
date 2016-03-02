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

import com.google.common.collect.Iterables;
import com.google.common.collect.Queues;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.framework.api.transaction.OperationType;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TestTransactionsNew extends BaseClassForTests
{
    @Test
    public void testErrors() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();
            CuratorOp createOp1 = client.transactionOp().create().forPath("/bar");
            CuratorOp createOp2 = client.transactionOp().create().forPath("/z/blue");
            final BlockingQueue<CuratorEvent> callbackQueue = new LinkedBlockingQueue<>();
            BackgroundCallback callback = new BackgroundCallback()
            {
                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
                {
                    callbackQueue.add(event);
                }
            };
            client.transaction().inBackground(callback).forOperations(createOp1, createOp2);
            CuratorEvent event = callbackQueue.poll(new Timing().milliseconds(), TimeUnit.MILLISECONDS);
            Assert.assertNotNull(event);
            Assert.assertNotNull(event.getOpResults());
            Assert.assertEquals(event.getOpResults().size(), 2);
            Assert.assertEquals(event.getOpResults().get(0).getError(), KeeperException.Code.OK.intValue());
            Assert.assertEquals(event.getOpResults().get(1).getError(), KeeperException.Code.NONODE.intValue());
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testCheckVersion() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();
            client.create().forPath("/foo");
            Stat stat = client.setData().forPath("/foo", "new".getBytes());  // up the version

            CuratorOp statOp = client.transactionOp().check().withVersion(stat.getVersion() + 1).forPath("/foo");
            CuratorOp createOp = client.transactionOp().create().forPath("/bar");
            try
            {
                client.transaction().forOperations(statOp, createOp);
                Assert.fail();
            }
            catch ( KeeperException.BadVersionException correct )
            {
                // correct
            }

            Assert.assertNull(client.checkExists().forPath("/bar"));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testWithNamespace() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.builder().connectString(server.getConnectString()).retryPolicy(new RetryOneTime(1)).namespace("galt").build();
        try
        {
            client.start();
            CuratorOp createOp1 = client.transactionOp().create().forPath("/foo", "one".getBytes());
            CuratorOp createOp2 = client.transactionOp().create().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath("/test-", "one".getBytes());
            CuratorOp setDataOp = client.transactionOp().setData().forPath("/foo", "two".getBytes());
            CuratorOp createOp3 = client.transactionOp().create().forPath("/foo/bar");
            CuratorOp deleteOp = client.transactionOp().delete().forPath("/foo/bar");

            Collection<CuratorTransactionResult> results = client.transaction().forOperations(createOp1, createOp2, setDataOp, createOp3, deleteOp);

            Assert.assertTrue(client.checkExists().forPath("/foo") != null);
            Assert.assertTrue(client.usingNamespace(null).checkExists().forPath("/galt/foo") != null);
            Assert.assertEquals(client.getData().forPath("/foo"), "two".getBytes());
            Assert.assertTrue(client.checkExists().forPath("/foo/bar") == null);

            CuratorTransactionResult ephemeralResult = Iterables.find(results, CuratorTransactionResult.ofTypeAndPath(OperationType.CREATE, "/test-"));
            Assert.assertNotNull(ephemeralResult);
            Assert.assertNotEquals(ephemeralResult.getResultPath(), "/test-");
            Assert.assertTrue(ephemeralResult.getResultPath().startsWith("/test-"));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testBasic() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();
            CuratorOp createOp1 = client.transactionOp().create().forPath("/foo");
            CuratorOp createOp2 = client.transactionOp().create().forPath("/foo/bar", "snafu".getBytes());

            Collection<CuratorTransactionResult> results = client.transaction().forOperations(createOp1, createOp2);

            Assert.assertTrue(client.checkExists().forPath("/foo/bar") != null);
            Assert.assertEquals(client.getData().forPath("/foo/bar"), "snafu".getBytes());

            CuratorTransactionResult fooResult = Iterables.find(results, CuratorTransactionResult.ofTypeAndPath(OperationType.CREATE, "/foo"));
            CuratorTransactionResult fooBarResult = Iterables.find(results, CuratorTransactionResult.ofTypeAndPath(OperationType.CREATE, "/foo/bar"));
            Assert.assertNotNull(fooResult);
            Assert.assertNotNull(fooBarResult);
            Assert.assertNotSame(fooResult, fooBarResult);
            Assert.assertEquals(fooResult.getResultPath(), "/foo");
            Assert.assertEquals(fooBarResult.getResultPath(), "/foo/bar");
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testBackground() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();
            CuratorOp createOp1 = client.transactionOp().create().forPath("/foo");
            CuratorOp createOp2 = client.transactionOp().create().forPath("/foo/bar", "snafu".getBytes());

            final BlockingQueue<List<CuratorTransactionResult>> queue = Queues.newLinkedBlockingQueue();
            BackgroundCallback callback = new BackgroundCallback()
            {
                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
                {
                    queue.add(event.getOpResults());
                }
            };
            client.transaction().inBackground(callback).forOperations(createOp1, createOp2);
            Collection<CuratorTransactionResult> results = queue.poll(5, TimeUnit.SECONDS);

            Assert.assertNotNull(results);
            Assert.assertTrue(client.checkExists().forPath("/foo/bar") != null);
            Assert.assertEquals(client.getData().forPath("/foo/bar"), "snafu".getBytes());

            CuratorTransactionResult fooResult = Iterables.find(results, CuratorTransactionResult.ofTypeAndPath(OperationType.CREATE, "/foo"));
            CuratorTransactionResult fooBarResult = Iterables.find(results, CuratorTransactionResult.ofTypeAndPath(OperationType.CREATE, "/foo/bar"));
            Assert.assertNotNull(fooResult);
            Assert.assertNotNull(fooBarResult);
            Assert.assertNotSame(fooResult, fooBarResult);
            Assert.assertEquals(fooResult.getResultPath(), "/foo");
            Assert.assertEquals(fooBarResult.getResultPath(), "/foo/bar");
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testBackgroundWithNamespace() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.builder().connectString(server.getConnectString()).retryPolicy(new RetryOneTime(1)).namespace("galt").build();
        try
        {
            client.start();
            CuratorOp createOp1 = client.transactionOp().create().forPath("/foo", "one".getBytes());
            CuratorOp createOp2 = client.transactionOp().create().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath("/test-", "one".getBytes());
            CuratorOp setDataOp = client.transactionOp().setData().forPath("/foo", "two".getBytes());
            CuratorOp createOp3 = client.transactionOp().create().forPath("/foo/bar");
            CuratorOp deleteOp = client.transactionOp().delete().forPath("/foo/bar");

            final BlockingQueue<List<CuratorTransactionResult>> queue = Queues.newLinkedBlockingQueue();
            BackgroundCallback callback = new BackgroundCallback()
            {
                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
                {
                    queue.add(event.getOpResults());
                }
            };
            client.transaction().inBackground(callback).forOperations(createOp1, createOp2, setDataOp, createOp3, deleteOp);

            Collection<CuratorTransactionResult> results = queue.poll(5, TimeUnit.SECONDS);

            Assert.assertNotNull(results);
            Assert.assertTrue(client.checkExists().forPath("/foo") != null);
            Assert.assertTrue(client.usingNamespace(null).checkExists().forPath("/galt/foo") != null);
            Assert.assertEquals(client.getData().forPath("/foo"), "two".getBytes());
            Assert.assertTrue(client.checkExists().forPath("/foo/bar") == null);

            CuratorTransactionResult ephemeralResult = Iterables.find(results, CuratorTransactionResult.ofTypeAndPath(OperationType.CREATE, "/test-"));
            Assert.assertNotNull(ephemeralResult);
            Assert.assertNotEquals(ephemeralResult.getResultPath(), "/test-");
            Assert.assertTrue(ephemeralResult.getResultPath().startsWith("/test-"));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }
}
