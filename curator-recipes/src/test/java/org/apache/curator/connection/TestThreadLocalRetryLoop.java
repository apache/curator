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
package org.apache.curator.connection;

import org.apache.curator.RetryPolicy;
import org.apache.curator.RetrySleeper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.compatibility.CuratorTestBase;
import org.apache.zookeeper.KeeperException;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TestThreadLocalRetryLoop extends CuratorTestBase
{
    private static final int retryCount = 4;

    @Test(description = "Check for fix for CURATOR-559")
    public void testRecursingRetry() throws Exception
    {
        AtomicInteger count = new AtomicInteger();
        try (CuratorFramework client = newClient(count))
        {
            prep(client);
            doLock(client);
            Assert.assertEquals(count.get(), retryCount + 1);    // Curator's retry policy has been off by 1 since inception - we might consider fixing it someday
        }
    }

    @Test(description = "Check for fix for CURATOR-559 with multiple threads")
    public void testThreadedRecursingRetry() throws Exception
    {
        final int threadQty = 4;
        ExecutorService executorService = Executors.newFixedThreadPool(threadQty);
        AtomicInteger count = new AtomicInteger();
        try (CuratorFramework client = newClient(count))
        {
            prep(client);
            for ( int i = 0; i < threadQty; ++i )
            {
                executorService.submit(() -> doLock(client));
            }
            executorService.shutdown();
            executorService.awaitTermination(timing.milliseconds(), TimeUnit.MILLISECONDS);
            Assert.assertEquals(count.get(), threadQty * (retryCount + 1));    // Curator's retry policy has been off by 1 since inception - we might consider fixing it someday
        }
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testBadReleaseWithNoGet()
    {
        ThreadLocalRetryLoop retryLoopStack = new ThreadLocalRetryLoop();
        retryLoopStack.release();
    }

    private CuratorFramework newClient(AtomicInteger count)
    {
        RetryPolicy retryPolicy = makeRetryPolicy(count);
        return CuratorFrameworkFactory.newClient(server.getConnectString(), 100, 100, retryPolicy);
    }

    private void prep(CuratorFramework client) throws Exception
    {
        client.start();
        client.create().forPath("/test");
        server.stop();
    }

    private Void doLock(CuratorFramework client) throws Exception
    {
        InterProcessReadWriteLock lock = new InterProcessReadWriteLock(client, "/test/lock");
        try
        {
            lock.readLock().acquire();
            Assert.fail("Should have thrown an exception");
        }
        catch ( KeeperException ignore )
        {
            // correct
        }
        return null;
    }

    private RetryPolicy makeRetryPolicy(AtomicInteger count)
    {
        return new RetryNTimes(retryCount, 1)
        {
            @Override
            public boolean allowRetry(int retryCount, long elapsedTimeMs, RetrySleeper sleeper)
            {
                count.incrementAndGet();
                return super.allowRetry(retryCount, elapsedTimeMs, sleeper);
            }
        };
    }
}
