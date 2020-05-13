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

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.zookeeper.ZooDefs.Ids.ANYONE_ID_UNSAFE;

public class TestCreate extends BaseClassForTests
{
    private static final List<ACL> READ_CREATE = Collections.singletonList(new ACL(ZooDefs.Perms.CREATE | ZooDefs.Perms.READ, ANYONE_ID_UNSAFE));
    private static final List<ACL> READ_CREATE_WRITE = Collections.singletonList(new ACL(ZooDefs.Perms.CREATE | ZooDefs.Perms.READ | ZooDefs.Perms.WRITE, ANYONE_ID_UNSAFE));

    private static ACLProvider testACLProvider = new ACLProvider()
    {
        @Override
        public List<ACL> getDefaultAcl()
        {
            return ZooDefs.Ids.OPEN_ACL_UNSAFE;
        }

        @Override
        public List<ACL> getAclForPath(String path)
        {
            switch (path)
            {
                case "/bar" : return READ_CREATE;
                case "/bar/foo" : return READ_CREATE_WRITE;
            }
            return null;
        }
    };

    private CuratorFramework createClient(ACLProvider aclProvider)
    {
        return CuratorFrameworkFactory.builder().
                aclProvider(aclProvider).
                connectString(server.getConnectString()).
                retryPolicy(new RetryOneTime(1)).
                build();
    }

    /**
     * Tests that the ACL list provided to the create builder is used for creating the parents.
     */
    @Test
    public void  testCreateWithParentsWithAcl() throws Exception
    {
        CuratorFramework client = createClient(new DefaultACLProvider());
        try
        {
            client.start();

            String path = "/bar/foo";
            List<ACL> acl = Collections.singletonList(new ACL(ZooDefs.Perms.CREATE | ZooDefs.Perms.READ, ANYONE_ID_UNSAFE));
            client.create().creatingParentsIfNeeded().withACL(acl).forPath(path);
            List<ACL> actual_bar_foo = client.getACL().forPath(path);
            Assert.assertEquals(actual_bar_foo, acl);
            List<ACL> actual_bar = client.getACL().forPath("/bar");
            Assert.assertEquals(actual_bar, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void  testCreateWithParentsWithAclApplyToParents() throws Exception
    {
        CuratorFramework client = createClient(new DefaultACLProvider());
        try
        {
            client.start();

            String path = "/bar/foo";
            List<ACL> acl = Collections.singletonList(new ACL(ZooDefs.Perms.CREATE | ZooDefs.Perms.READ, ANYONE_ID_UNSAFE));
            client.create().creatingParentsIfNeeded().withACL(acl, true).forPath(path);
            List<ACL> actual_bar_foo = client.getACL().forPath(path);
            Assert.assertEquals(actual_bar_foo, acl);
            List<ACL> actual_bar = client.getACL().forPath("/bar");
            Assert.assertEquals(actual_bar, acl);
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    /**
     * Tests that the ACL list provided to the create builder is used for creating the parents.
     */
    @Test
    public void  testCreateWithParentsWithAclInBackground() throws Exception
    {
        CuratorFramework client = createClient(new DefaultACLProvider());
        try
        {
            client.start();
            final CountDownLatch latch = new CountDownLatch(1);
            String path = "/bar/foo";
            List<ACL> acl = Collections.singletonList(new ACL(ZooDefs.Perms.CREATE | ZooDefs.Perms.READ, ANYONE_ID_UNSAFE));
            BackgroundCallback callback = new BackgroundCallback()
            {
                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
                {
                    latch.countDown();
                }
            };
            client.create().creatingParentsIfNeeded().withACL(acl).inBackground(callback).forPath(path);
            Assert.assertTrue(latch.await(2000, TimeUnit.MILLISECONDS), "Callback not invoked");
            List<ACL> actual_bar_foo = client.getACL().forPath(path);
            Assert.assertEquals(actual_bar_foo, acl);
            List<ACL> actual_bar = client.getACL().forPath("/bar");
            Assert.assertEquals(actual_bar, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void  testCreateWithParentsWithAclApplyToParentsInBackground() throws Exception
    {
        CuratorFramework client = createClient(new DefaultACLProvider());
        try
        {
            client.start();
            final CountDownLatch latch = new CountDownLatch(1);
            String path = "/bar/foo";
            List<ACL> acl = Collections.singletonList(new ACL(ZooDefs.Perms.CREATE | ZooDefs.Perms.READ, ANYONE_ID_UNSAFE));
            BackgroundCallback callback = new BackgroundCallback()
            {
                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
                {
                    latch.countDown();
                }
            };
            client.create().creatingParentsIfNeeded().withACL(acl, true).inBackground(callback).forPath(path);
            Assert.assertTrue(latch.await(2000, TimeUnit.MILLISECONDS), "Callback not invoked");
            List<ACL> actual_bar_foo = client.getACL().forPath(path);
            Assert.assertEquals(actual_bar_foo, acl);
            List<ACL> actual_bar = client.getACL().forPath("/bar");
            Assert.assertEquals(actual_bar, acl);
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    /**
     * Tests that if no ACL list provided to the create builder, then the ACL list is created based on the client's ACLProvider.
     */
    @Test
    public void  testCreateWithParentsWithoutAcl() throws Exception
    {
        CuratorFramework client = createClient(testACLProvider);
        try
        {
            client.start();

            String path = "/bar/foo/boo";
            client.create().creatingParentsIfNeeded().forPath(path);
            List<ACL> actual_bar_foo_boo = client.getACL().forPath("/bar/foo/boo");
            Assert.assertEquals(actual_bar_foo_boo, ZooDefs.Ids.OPEN_ACL_UNSAFE);
            List<ACL> actual_bar_foo = client.getACL().forPath("/bar/foo");
            Assert.assertEquals(actual_bar_foo, READ_CREATE_WRITE);
            List<ACL> actual_bar = client.getACL().forPath("/bar");
            Assert.assertEquals(actual_bar, READ_CREATE);
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    /**
     * Tests that if no ACL list provided to the create builder, then the ACL list is created based on the client's ACLProvider.
     */
    @Test
    public void  testCreateWithParentsWithoutAclInBackground() throws Exception
    {
        CuratorFramework client = createClient(testACLProvider);
        try
        {
            client.start();

            final CountDownLatch latch = new CountDownLatch(1);
            BackgroundCallback callback = new BackgroundCallback()
            {
                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
                {
                    latch.countDown();
                }
            };

            final String path = "/bar/foo/boo";
            client.create().creatingParentsIfNeeded().inBackground(callback).forPath(path);
            Assert.assertTrue(latch.await(2000, TimeUnit.MILLISECONDS), "Callback not invoked");
            List<ACL> actual_bar_foo_boo = client.getACL().forPath(path);
            Assert.assertEquals(actual_bar_foo_boo, ZooDefs.Ids.OPEN_ACL_UNSAFE);
            List<ACL> actual_bar_foo = client.getACL().forPath("/bar/foo");
            Assert.assertEquals(actual_bar_foo, READ_CREATE_WRITE);
            List<ACL> actual_bar = client.getACL().forPath("/bar");
            Assert.assertEquals(actual_bar, READ_CREATE);
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testCreateProtectedUtils() throws Exception
    {
        try (CuratorFramework client = CuratorFrameworkFactory.builder().
                connectString(server.getConnectString()).
                retryPolicy(new RetryOneTime(1)).
                build())
        {
            client.start();
            client.blockUntilConnected();
            client.create().forPath("/parent");
            Assert.assertEquals(client.getChildren().forPath("/parent").size(), 0);
            client.create().withProtection().withMode(CreateMode.EPHEMERAL).forPath("/parent/test");
            final List<String> children = client.getChildren().forPath("/parent");
            Assert.assertEquals(1, children.size());
            final String testZNodeName = children.get(0);
            Assert.assertEquals(testZNodeName.length(), ProtectedUtils.PROTECTED_PREFIX_WITH_UUID_LENGTH + "test".length());
            Assert.assertTrue(testZNodeName.startsWith(ProtectedUtils.PROTECTED_PREFIX));
            Assert.assertEquals(testZNodeName.charAt(ProtectedUtils.PROTECTED_PREFIX_WITH_UUID_LENGTH-1), ProtectedUtils.PROTECTED_SEPARATOR);
            Assert.assertTrue(ProtectedUtils.isProtectedZNode(testZNodeName));
            Assert.assertEquals(ProtectedUtils.normalize(testZNodeName), "test");
            Assert.assertFalse(ProtectedUtils.isProtectedZNode("parent"));
            Assert.assertEquals(ProtectedUtils.normalize("parent"), "parent");
        }
    }
    
    @Test
    public void testProtectedUtils() throws Exception
    {
        String name = "_c_53345f98-9423-4e0c-a7b5-9f819e3ec2e1-yo";
        Assert.assertTrue(ProtectedUtils.isProtectedZNode(name));
        Assert.assertEquals(ProtectedUtils.normalize(name), "yo");
        name = "c_53345f98-9423-4e0c-a7b5-9f819e3ec2e1-yo";
        Assert.assertFalse(ProtectedUtils.isProtectedZNode(name));
        Assert.assertEquals(ProtectedUtils.normalize(name), name);
        name = "_c_53345f98-hola-4e0c-a7b5-9f819e3ec2e1-yo";
        Assert.assertFalse(ProtectedUtils.isProtectedZNode(name));
        Assert.assertEquals(ProtectedUtils.normalize(name), name);
        name = "_c_53345f98-hola-4e0c-a7b5-9f819e3ec2e1+yo";
        Assert.assertFalse(ProtectedUtils.isProtectedZNode(name));
        Assert.assertEquals(ProtectedUtils.normalize(name), name);
    }
}
