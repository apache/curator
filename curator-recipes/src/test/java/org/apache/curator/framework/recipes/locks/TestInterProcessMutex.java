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

import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.TestCleanState;
import org.apache.curator.framework.schema.Schema;
import org.apache.curator.framework.schema.SchemaSet;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.Compatibility;
import org.apache.zookeeper.CreateMode;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class TestInterProcessMutex extends TestInterProcessMutexBase
{
    private static final String LOCK_PATH = LOCK_BASE_PATH + "/our-lock";

    @Override
    protected InterProcessLock makeLock(CuratorFramework client)
    {
        return new InterProcessMutex(client, LOCK_PATH);
    }

    @Test
    public void testWithSchema() throws Exception
    {
        Schema schemaRoot = Schema.builderForRecipeParent("/foo").name("root").build();
        Schema schemaLocks = Schema.builderForRecipe("/foo").name("locks").build();
        SchemaSet schemaSet = new SchemaSet(Lists.newArrayList(schemaRoot, schemaLocks), false);
        CuratorFramework client = CuratorFrameworkFactory.builder()
            .connectString(server.getConnectString())
            .retryPolicy(new RetryOneTime(1))
            .schemaSet(schemaSet)
            .build();
        try
        {
            client.start();

            InterProcessMutex lock = new InterProcessMutex(client, "/foo");
            lock.acquire();
            lock.release();
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testRevoking() throws Exception
    {
        final CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();
            final InterProcessMutex lock = new InterProcessMutex(client, LOCK_PATH);

            ExecutorService executorService = Executors.newCachedThreadPool();

            final CountDownLatch revokeLatch = new CountDownLatch(1);
            final CountDownLatch lockLatch = new CountDownLatch(1);
            Future<Void> f1 = executorService.submit
                (
                    new Callable<Void>()
                    {
                        @Override
                        public Void call() throws Exception
                        {
                            RevocationListener<InterProcessMutex> listener = new RevocationListener<InterProcessMutex>()
                            {
                                @Override
                                public void revocationRequested(InterProcessMutex lock)
                                {
                                    revokeLatch.countDown();
                                }
                            };
                            lock.makeRevocable(listener);
                            lock.acquire();
                            lockLatch.countDown();
                            revokeLatch.await();
                            lock.release();
                            return null;
                        }
                    }
                );

            Future<Void> f2 = executorService.submit
                (
                    new Callable<Void>()
                    {
                        @Override
                        public Void call() throws Exception
                        {
                            Assert.assertTrue(lockLatch.await(10, TimeUnit.SECONDS));
                            Collection<String> nodes = lock.getParticipantNodes();
                            Assert.assertEquals(nodes.size(), 1);
                            Revoker.attemptRevoke(client, nodes.iterator().next());

                            InterProcessMutex l2 = new InterProcessMutex(client, LOCK_PATH);
                            Assert.assertTrue(l2.acquire(5, TimeUnit.SECONDS));
                            l2.release();
                            return null;
                        }
                    }
                );

            f2.get();
            f1.get();
        }
        finally
        {
            TestCleanState.closeAndTestClean(client);
        }
    }

    @Test
    public void testPersistentLock() throws Exception
    {
        final CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();

        try
        {
            final InterProcessMutex lock = new InterProcessMutex(client, LOCK_PATH, new StandardLockInternalsDriver()
            {
                @Override
                public String createsTheLock(CuratorFramework client, String path, byte[] lockNodeBytes) throws Exception
                {
                    String ourPath;
                    if ( lockNodeBytes != null )
                    {
                        ourPath = client.create().creatingParentsIfNeeded().withProtection().withMode(CreateMode.PERSISTENT).forPath(path, lockNodeBytes);
                    }
                    else
                    {
                        ourPath = client.create().creatingParentsIfNeeded().withProtection().withMode(CreateMode.PERSISTENT).forPath(path);
                    }
                    return ourPath;
                }
            });

            // Get a persistent lock
            lock.acquire(10, TimeUnit.SECONDS);
            Assert.assertTrue(lock.isAcquiredInThisProcess());

            // Kill the session, check that lock node still exists
            Compatibility.injectSessionExpiration(client.getZookeeperClient().getZooKeeper());
            Assert.assertNotNull(client.checkExists().forPath(LOCK_PATH));

            // Release the lock and verify that the actual lock node created no longer exists
            String actualLockPath = lock.getLockPath();
            lock.release();
            Assert.assertNull(client.checkExists().forPath(actualLockPath));
        }
        finally
        {
            TestCleanState.closeAndTestClean(client);
        }
    }
}
