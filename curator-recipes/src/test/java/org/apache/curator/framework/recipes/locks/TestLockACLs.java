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

package org.apache.curator.framework.recipes.locks;

import com.google.common.io.Closeables;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.recipes.BaseClassForTests;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.Collections;
import java.util.List;

public class TestLockACLs extends BaseClassForTests
{
    private static final List<ACL> ACLS = Collections.singletonList(new ACL(ZooDefs.Perms.ALL, new Id("ip", "127.0.0.1")));

    private CuratorFramework createClient() throws Exception
    {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.builder()
            .namespace("ns")
            .connectString(server.getConnectString())
            .retryPolicy(retryPolicy)
            .aclProvider(new MyACLProvider())
            .build();
        client.start();
        return client;
    }

    @Test
    public void testLockACLs() throws Exception
    {
        CuratorFramework client = createClient();
        try
        {
            client.create().forPath("/foo");
            Assert.assertNotNull(client.checkExists().forPath("/foo"));
            Assert.assertEquals(ZooDefs.Perms.ALL, client.getACL().forPath("/foo").get(0).getPerms());
            Assert.assertEquals("ip", client.getACL().forPath("/foo").get(0).getId().getScheme());
            Assert.assertEquals("127.0.0.1", client.getACL().forPath("/foo").get(0).getId().getId());

            InterProcessReadWriteLock lock = new InterProcessReadWriteLock(client, "/bar");
            InterProcessMutex writeLock = lock.writeLock();
            writeLock.acquire();
            Assert.assertNotNull(client.checkExists().forPath("/bar"));
            Assert.assertEquals(ZooDefs.Perms.ALL, client.getACL().forPath("/bar").get(0).getPerms());
            Assert.assertEquals("ip", client.getACL().forPath("/bar").get(0).getId().getScheme());
            Assert.assertEquals("127.0.0.1", client.getACL().forPath("/bar").get(0).getId().getId());
        }
        finally
        {
            Closeables.closeQuietly(client);
        }
    }

    public class MyACLProvider implements ACLProvider
    {

        @Override
        public List<ACL> getDefaultAcl()
        {
            return ACLS;
        }

        @Override
        public List<ACL> getAclForPath(String path)
        {
            return ACLS;
        }
    }
}