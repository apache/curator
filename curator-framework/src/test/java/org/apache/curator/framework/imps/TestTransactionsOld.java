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
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.framework.api.transaction.OperationType;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.Collection;

@SuppressWarnings("deprecation")
public class TestTransactionsOld extends BaseClassForTests
{
    @Test
    public void     testCheckVersion() throws Exception
    {
        CuratorFramework        client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();
            client.create().forPath("/foo");
            Stat        stat = client.setData().forPath("/foo", "new".getBytes());  // up the version

            try
            {
                client.inTransaction()
                    .check().withVersion(stat.getVersion() + 1).forPath("/foo") // force a bad version
                .and()
                    .create().forPath("/bar")
                .and()
                    .commit();

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
    public void     testWithNamespace() throws Exception
    {
        CuratorFramework        client = CuratorFrameworkFactory.builder().connectString(server.getConnectString()).retryPolicy(new RetryOneTime(1)).namespace("galt").build();
        try
        {
            client.start();
            Collection<CuratorTransactionResult>    results =
                client.inTransaction()
                    .create().forPath("/foo", "one".getBytes())
                .and()
                    .create().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath("/test-", "one".getBytes())
                .and()
                    .setData().forPath("/foo", "two".getBytes())
                .and()
                    .create().forPath("/foo/bar")
                .and()
                    .delete().forPath("/foo/bar")
                .and()
                    .commit();

            Assert.assertTrue(client.checkExists().forPath("/foo") != null);
            Assert.assertTrue(client.usingNamespace(null).checkExists().forPath("/galt/foo") != null);
            Assert.assertEquals(client.getData().forPath("/foo"), "two".getBytes());
            Assert.assertTrue(client.checkExists().forPath("/foo/bar") == null);

            CuratorTransactionResult    ephemeralResult = Iterables.find(results, CuratorTransactionResult.ofTypeAndPath(OperationType.CREATE, "/test-"));
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
    public void     testWithCompression() throws Exception
    {
        CuratorFramework        client = CuratorFrameworkFactory.builder().connectString(server.getConnectString()).retryPolicy(new RetryOneTime(1)).namespace("galt").build();
        client.start();
        try
        {
            Collection<CuratorTransactionResult>    results =
                    client.inTransaction()
                        .create().compressed().forPath("/foo", "one".getBytes())
                    .and()
                        .create().compressed().withACL(ZooDefs.Ids.READ_ACL_UNSAFE).forPath("/bar", "two".getBytes())
                    .and()
                        .create().compressed().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath("/test-", "three".getBytes())
                    .and()
                        .create().compressed().withMode(CreateMode.PERSISTENT).withACL(ZooDefs.Ids.READ_ACL_UNSAFE).forPath("/baz", "four".getBytes())
                    .and()
                        .setData().compressed().withVersion(0).forPath("/foo", "five".getBytes())
                    .and()
                        .commit();

            Assert.assertTrue(client.checkExists().forPath("/foo") != null);
            Assert.assertEquals(client.getData().decompressed().forPath("/foo"), "five".getBytes());

            Assert.assertTrue(client.checkExists().forPath("/bar") != null);
            Assert.assertEquals(client.getData().decompressed().forPath("/bar"), "two".getBytes());
            Assert.assertEquals(client.getACL().forPath("/bar"), ZooDefs.Ids.READ_ACL_UNSAFE);

            CuratorTransactionResult    ephemeralResult = Iterables.find(results, CuratorTransactionResult.ofTypeAndPath(OperationType.CREATE, "/test-"));
            Assert.assertNotNull(ephemeralResult);
            Assert.assertNotEquals(ephemeralResult.getResultPath(), "/test-");
            Assert.assertTrue(ephemeralResult.getResultPath().startsWith("/test-"));

            Assert.assertTrue(client.checkExists().forPath("/baz") != null);
            Assert.assertEquals(client.getData().decompressed().forPath("/baz"), "four".getBytes());
            Assert.assertEquals(client.getACL().forPath("/baz"), ZooDefs.Ids.READ_ACL_UNSAFE);
        }
        finally
        {
            client.close();
        }
    }

    @Test
    public void     testBasic() throws Exception
    {
        CuratorFramework        client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();
            Collection<CuratorTransactionResult>    results =
                client.inTransaction()
                    .create().forPath("/foo")
                .and()
                    .create().forPath("/foo/bar", "snafu".getBytes())
                .and()
                    .commit();

            Assert.assertTrue(client.checkExists().forPath("/foo/bar") != null);
            Assert.assertEquals(client.getData().forPath("/foo/bar"), "snafu".getBytes());

            CuratorTransactionResult    fooResult = Iterables.find(results, CuratorTransactionResult.ofTypeAndPath(OperationType.CREATE, "/foo"));
            CuratorTransactionResult    fooBarResult = Iterables.find(results, CuratorTransactionResult.ofTypeAndPath(OperationType.CREATE, "/foo/bar"));
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
}
