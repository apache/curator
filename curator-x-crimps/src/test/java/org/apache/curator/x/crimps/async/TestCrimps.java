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
package org.apache.curator.x.crimps.async;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundPathAndBytesable;
import org.apache.curator.framework.api.BackgroundPathable;
import org.apache.curator.framework.api.BackgroundVersionable;
import org.apache.curator.framework.api.ChildrenDeletable;
import org.apache.curator.framework.api.DeleteBuilderMain;
import org.apache.curator.framework.api.SetDataBackgroundVersionable;
import org.apache.curator.framework.api.SetDataBuilder;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.framework.api.transaction.OperationType;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.Timing;
import org.apache.curator.x.crimps.Crimps;
import org.apache.curator.x.crimps.async.details.AsyncCrimps;
import org.apache.curator.x.crimps.async.details.Crimped;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

public class TestCrimps extends BaseClassForTests
{
    private final AsyncCrimps async = Crimps.async();

    @Test
    public void testCreateAndSet() throws Exception
    {
        try ( CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1)) )
        {
            client.start();

            SetDataBuilder setDataBuilder = client.setData();
            BackgroundPathAndBytesable<Stat> withVersion = client.setData().withVersion(0);
            SetDataBackgroundVersionable compressed = client.setData().compressed();

            CompletionStage<String> f = async.name(client.create().creatingParentContainersIfNeeded().withMode(CreateMode.PERSISTENT_SEQUENTIAL)).forPath("/a/b/c");
            complete(f.handle((path, e) -> {
                Assert.assertEquals(path, "/a/b/c0000000000");
                return null;
            }));

            f = async.name(client.create()).forPath("/foo/bar");
            assertException(f, KeeperException.Code.NONODE);

            CompletionStage<Stat> statFuture = async.statBytes(client.setData()).forPath("/a/b/c0000000000", "hey".getBytes());
            complete(statFuture.handle((stat, e) -> {
                Assert.assertNotNull(stat);
                return null;
            }));
        }
    }

    @Test
    public void testDelete() throws Exception
    {
        try ( CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1)) )
        {
            client.start();
            client.create().forPath("/test");

            CompletionStage<Void> f = async.ignored(client.delete()).forPath("/test");
            complete(f.handle((v, e) -> {
                Assert.assertEquals(v, null);
                Assert.assertEquals(e, null);
                return null;
            }));

            f = async.ignored(client.delete()).forPath("/test");
            assertException(f, KeeperException.Code.NONODE);
        }
    }

    @Test
    public void testGetData() throws Exception
    {
        try ( CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1)) )
        {
            client.start();
            client.create().forPath("/test", "foo".getBytes());

            CompletionStage<byte[]> f = async.data(client.getData()).forPath("/test");
            complete(f.handle((data, e) -> {
                Assert.assertEquals(data, "foo".getBytes());
                return null;
            }));
        }
    }

    @Test
    public void testExists() throws Exception
    {
        try ( CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1)) )
        {
            client.start();

            CompletionStage<Stat> f = async.stat(client.checkExists()).forPath("/test");
            complete(f.handle((stat, e) -> {
                Assert.assertNull(e);
                Assert.assertNull(stat);
                return null;
            }));

            async.path(client.create()).forPath("/test").toCompletableFuture().get();
            f = async.stat(client.checkExists()).forPath("/test");
            complete(f.handle((stat, e) -> {
                Assert.assertNull(e);
                Assert.assertNotNull(stat);
                return null;
            }));
        }
    }

    @Test
    public void testExistsWatched() throws Exception
    {
        try ( CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1)) )
        {
            client.start();

            Crimped<Stat> crimped = async.statWatched(client.checkExists().creatingParentContainersIfNeeded()).forPath("/one/two");
            CountDownLatch latch = new CountDownLatch(1);
            crimped.event().handle((event, e) -> {
                Assert.assertNotNull(event);
                latch.countDown();
                return null;
            });

            async.path(client.create()).forPath("/one/two");

            Assert.assertTrue(new Timing().awaitLatch(latch));
        }
    }

    @Test
    public void testReconfig() throws Exception
    {
        try ( CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1)) )
        {
            client.start();

            CompletionStage<byte[]> f = async.ensemble(client.reconfig(), Arrays.asList("1", "2"), Arrays.asList("3", "4")).forEnsemble();
            assertException(f, KeeperException.Code.UNIMPLEMENTED);
        }
    }

    @Test
    public void testGetConfig() throws Exception
    {
        try ( CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1)) )
        {
            client.start();

            CompletionStage<byte[]> f = async.ensemble(client.getConfig()).forEnsemble();
            complete(f.handle((data, e) -> {
                Assert.assertNotNull(data);
                return null;
            }));
        }
    }

    @Test
    public void testMulti() throws Exception
    {
        try ( CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1)) )
        {
            client.start();

            CuratorOp createA = client.transactionOp().create().forPath("/a");
            CuratorOp createB = client.transactionOp().create().forPath("/b");

            CompletionStage<List<CuratorTransactionResult>> f = async.opResults(client.transaction()).forOperations(Arrays.asList(createA, createB));
            complete(f.handle((ops, e) -> {
                Assert.assertNotNull(ops);
                Assert.assertEquals(ops.get(0).getType(), OperationType.CREATE);
                Assert.assertEquals(ops.get(0).getForPath(), "/a");
                Assert.assertEquals(ops.get(1).getType(), OperationType.CREATE);
                Assert.assertEquals(ops.get(1).getForPath(), "/b");
                return null;
            }));
        }
    }

    public void assertException(CompletionStage<?> f, KeeperException.Code code) throws Exception
    {
        complete(f.handle((value, e) -> {
            if ( e == null )
            {
                Assert.fail(code + " expected");
            }
            KeeperException keeperException = unwrap(e);
            Assert.assertNotNull(keeperException);
            Assert.assertEquals(keeperException.code(), code);
            return null;
        }));
    }

    private void complete(CompletionStage<?> f) throws Exception
    {
        try
        {
            f.toCompletableFuture().get();
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
        }
        catch ( ExecutionException e )
        {
            if ( e.getCause() instanceof AssertionError )
            {
                throw ((AssertionError)e.getCause());
            }
            throw e;
        }
    }

    private static KeeperException unwrap(Throwable e)
    {
        while ( e != null )
        {
            if ( e instanceof KeeperException )
            {
                return (KeeperException)e;
            }
            e = e.getCause();
        }
        return null;
    }
}
