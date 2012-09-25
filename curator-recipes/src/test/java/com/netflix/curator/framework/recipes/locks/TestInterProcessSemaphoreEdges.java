/*
 * Copyright 2012 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.curator.framework.recipes.locks;

import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.recipes.BaseClassForTests;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.netflix.curator.test.KillSession;
import com.netflix.curator.test.Timing;
import junit.framework.Assert;
import org.testng.annotations.Test;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TestInterProcessSemaphoreEdges extends BaseClassForTests
{
    @Test(enabled = false)  // currently failing TODO
    public void     testSessionExpiration1Instance() throws Exception
    {
        final int       QTY = 100;

        final Timing                        timing = new Timing();
        ExecutorService                     executor = Executors.newCachedThreadPool();
        CuratorFramework                    client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new ExponentialBackoffRetry(timing.milliseconds(), 100));  // retry until it succeeds
        final InterProcessSemaphoreV2       semaphore = new InterProcessSemaphoreV2(client, "/test", 1);
        try
        {
            client.start();

            final CountDownLatch                hasSemaphoreLatch = new CountDownLatch(1);
            final CountDownLatch                latch = new CountDownLatch(1);
            ExecutorCompletionService<Void>     completionService = new ExecutorCompletionService<Void>(executor);
            for ( int i = 0; i < QTY; ++i )
            {
                completionService.submit
                (
                    new Callable<Void>()
                    {
                        @Override
                        public Void call() throws Exception
                        {
                            Thread.sleep((int)(Math.random() * 10));
                            Lease lease = semaphore.acquire(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS);
                            Assert.assertNotNull(lease);
                            hasSemaphoreLatch.countDown();
                            try
                            {
                                latch.await();
                            }
                            finally
                            {
                                lease.close();
                            }
                            return null;
                        }
                    }
                );
            }

            hasSemaphoreLatch.await();
            KillSession.kill(client.getZookeeperClient().getZooKeeper(), server.getConnectString());
            latch.countDown();

            for ( int i = 0; i < QTY; ++i )
            {
                try
                {
                    completionService.take().get(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS);   // should all complete without an exception
                }
                catch ( TimeoutException e )
                {
                    Assert.fail("Timed out waiting for latch to re-acquire");
                }
            }
        }
        finally
        {
            executor.shutdown();
            Closeables.closeQuietly(client);
        }
    }

    @Test
    public void     testSessionExpiration() throws Exception
    {
        final int       QTY = 100;

        final Timing                        timing = new Timing();
        ExecutorService                     executor = Executors.newCachedThreadPool();
        CuratorFramework                    client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new ExponentialBackoffRetry(timing.milliseconds(), 100));  // retry until it succeeds
        try
        {
            client.start();

            List<InterProcessSemaphoreV2> semaphores = Lists.newArrayList();
            for ( int i = 0; i < QTY; ++i )
            {
                InterProcessSemaphoreV2 semaphore = new InterProcessSemaphoreV2(client, "/test", 1);
                semaphores.add(semaphore);
            }

            final CountDownLatch                hasSemaphoreLatch = new CountDownLatch(1);
            final CountDownLatch                latch = new CountDownLatch(1);
            ExecutorCompletionService<Void>     completionService = new ExecutorCompletionService<Void>(executor);
            for ( final InterProcessSemaphoreV2 semaphore : semaphores )
            {
                completionService.submit
                (
                    new Callable<Void>()
                    {
                        @Override
                        public Void call() throws Exception
                        {
                            Thread.sleep((int)(Math.random() * 10));
                            Lease lease = semaphore.acquire(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS);
                            Assert.assertNotNull(lease);
                            hasSemaphoreLatch.countDown();
                            try
                            {
                                latch.await();
                            }
                            finally
                            {
                                lease.close();
                            }
                            return null;
                        }
                    }
                );
            }

            hasSemaphoreLatch.await();
            KillSession.kill(client.getZookeeperClient().getZooKeeper(), server.getConnectString());
            latch.countDown();

            for ( int i = 0; i < QTY; ++i )
            {
                completionService.take().get();   // should all complete without an exception
            }
        }
        finally
        {
            executor.shutdown();
            Closeables.closeQuietly(client);
        }
    }
}
