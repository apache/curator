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
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.Arrays;
import java.util.concurrent.CompletionStage;
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
    public void testReconfig() throws Exception
    {
        try ( CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1)) )
        {
            client.start();

            CompletionStage<byte[]> f = async.joiningLeaving(client.reconfig(), Arrays.asList("1", "2"), Arrays.asList("3", "4")).forEnsemble();
            assertException(f, KeeperException.Code.UNIMPLEMENTED);
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
