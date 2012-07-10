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
import com.netflix.curator.framework.state.ConnectionState;
import com.netflix.curator.framework.state.ConnectionStateListener;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.netflix.curator.test.InstanceSpec;
import com.netflix.curator.test.TestingCluster;
import com.netflix.curator.test.Timing;
import junit.framework.Assert;
import org.testng.annotations.Test;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class TestInterProcessSemaphoreCluster
{
    @Test
    public void     testCluster() throws Exception
    {
        final int           QTY = 20;
        final int           OPERATION_TIME_MS = 1000;
        final String        PATH = "/foo/bar/lock";

        ExecutorService                 executorService = Executors.newFixedThreadPool(QTY);
        ExecutorCompletionService<Void> completionService = new ExecutorCompletionService<Void>(executorService);
        final Timing                    timing = new Timing();
        TestingCluster                  cluster = new TestingCluster(3);
        try
        {
            cluster.start();

            final AtomicInteger         opCount = new AtomicInteger(0);
            List<SemaphoreClient>       semaphoreClients = Lists.newArrayList();
            for ( int i = 0; i < QTY; ++i )
            {
                SemaphoreClient semaphoreClient = new SemaphoreClient
                (
                    cluster.getConnectString(),
                    PATH,
                    new Callable<Void>()
                    {
                        @Override
                        public Void call() throws Exception
                        {
                            opCount.incrementAndGet();
                            Thread.sleep(OPERATION_TIME_MS);
                            return null;
                        }
                    }
                );
                completionService.submit(semaphoreClient);
                semaphoreClients.add(semaphoreClient);
            }

            timing.forWaiting().sleepABit();

            Assert.assertNotNull(SemaphoreClient.getActiveClient());

            final CountDownLatch    latch = new CountDownLatch(1);
            CuratorFramework        client = CuratorFrameworkFactory.newClient(cluster.getConnectString(), timing.session(), timing.connection(), new ExponentialBackoffRetry(100, 3));
            ConnectionStateListener listener = new ConnectionStateListener()
            {
                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState)
                {
                    if ( newState == ConnectionState.LOST )
                    {
                        latch.countDown();
                    }
                }
            };
            client.getConnectionStateListenable().addListener(listener);
            client.start();
            client.getZookeeperClient().blockUntilConnectedOrTimedOut();

            cluster.stop();
            try
            {
                latch.await();
            }
            finally
            {
                Closeables.closeQuietly(client);
            }

            long        startTicks = System.currentTimeMillis();
            for(;;)
            {
                int     thisOpCount = opCount.get();
                Thread.sleep(2 * OPERATION_TIME_MS);
                if ( thisOpCount == opCount.get() )
                {
                    break;  // checking that the op count isn't increasing
                }
                Assert.assertTrue((System.currentTimeMillis() - startTicks) < timing.forWaiting().milliseconds());
            }

            int     thisOpCount = opCount.get();

            Iterator<InstanceSpec> iterator = cluster.getInstances().iterator();
            cluster = new TestingCluster(iterator.next(), iterator.next());
            cluster.start();
            timing.forWaiting().sleepABit();

            startTicks = System.currentTimeMillis();
            for(;;)
            {
                Thread.sleep(2 * OPERATION_TIME_MS);
                if ( opCount.get() > thisOpCount )
                {
                    break;  // checking that semaphore has started working again
                }
                Assert.assertTrue((System.currentTimeMillis() - startTicks) < timing.forWaiting().milliseconds());
            }
        }
        finally
        {
            Closeables.closeQuietly(cluster);
            executorService.shutdownNow();
        }
    }
}
