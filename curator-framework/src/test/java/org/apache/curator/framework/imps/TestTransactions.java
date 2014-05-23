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
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.Collection;

public class TestTransactions extends BaseClassForTests
{
    @Test
    public void     testCheckVersion() throws Exception
    {
        CuratorFramework        client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
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
            client.close();
        }
    }

    @Test
    public void     testWithNamespace() throws Exception
    {
        CuratorFramework        client = CuratorFrameworkFactory.builder().connectString(server.getConnectString()).retryPolicy(new RetryOneTime(1)).namespace("galt").build();
        client.start();
        try
        {
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
            Assert.assertTrue(client.nonNamespaceView().checkExists().forPath("/galt/foo") != null);
            Assert.assertEquals(client.getData().forPath("/foo"), "two".getBytes());
            Assert.assertTrue(client.checkExists().forPath("/foo/bar") == null);

            CuratorTransactionResult    ephemeralResult = Iterables.find(results, CuratorTransactionResult.ofTypeAndPath(OperationType.CREATE, "/test-"));
            Assert.assertNotNull(ephemeralResult);
            Assert.assertNotEquals(ephemeralResult.getResultPath(), "/test-");
            Assert.assertTrue(ephemeralResult.getResultPath().startsWith("/test-"));
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
        client.start();
        try
        {
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
            client.close();
        }
    }
}
