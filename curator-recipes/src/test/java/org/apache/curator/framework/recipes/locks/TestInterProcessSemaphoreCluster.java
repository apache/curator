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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.google.common.collect.Lists;
import io.github.artsok.RepeatedIfExceptionsTest;
import org.apache.curator.ensemble.EnsembleProvider;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.TestCleanState;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingCluster;
import org.apache.curator.test.Timing;
import org.apache.curator.test.compatibility.CuratorTestBase;
import org.apache.curator.utils.CloseableUtils;
import org.junit.jupiter.api.Tag;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@Tag(CuratorTestBase.zk35TestCompatibilityGroup)
public class TestInterProcessSemaphoreCluster extends BaseClassForTests
{
    @RepeatedIfExceptionsTest(repeats = BaseClassForTests.REPEATS)
    public void     testKilledServerWithEnsembleProvider() throws Exception
    {
        final int           CLIENT_QTY = 10;
        final Timing        timing = new Timing();
        final String        PATH = "/foo/bar/lock";

        ExecutorService                 executorService = Executors.newFixedThreadPool(CLIENT_QTY);
        ExecutorCompletionService<Void> completionService = new ExecutorCompletionService<Void>(executorService);
        TestingCluster                  cluster = createAndStartCluster(3);
        try
        {
            final AtomicReference<String>   connectionString = new AtomicReference<String>(cluster.getConnectString());
            final EnsembleProvider          provider = new EnsembleProvider()
            {
                @Override
                public void setConnectionString(String connectionString)
                {
                }

                @Override
                public boolean updateServerListEnabled()
                {
                    return false;
                }

                @Override
                public void start() throws Exception
                {
                }

                @Override
                public String getConnectionString()
                {
                    return connectionString.get();
                }

                @Override
                public void close() throws IOException
                {
                }
            };

            final Semaphore             acquiredSemaphore = new Semaphore(0);
            final AtomicInteger         acquireCount = new AtomicInteger(0);
            final CountDownLatch        suspendedLatch = new CountDownLatch(CLIENT_QTY);
            for ( int i = 0; i < CLIENT_QTY; ++i )
            {
                completionService.submit
                (
                    new Callable<Void>()
                    {
                        @Override
                        public Void call() throws Exception
                        {
                            CuratorFramework        client = CuratorFrameworkFactory.builder()
                                .ensembleProvider(provider)
                                .sessionTimeoutMs(timing.session())
                                .connectionTimeoutMs(timing.connection())
                                .retryPolicy(new ExponentialBackoffRetry(100, 3))
                                .build();
                            try
                            {
                                final Semaphore     suspendedSemaphore = new Semaphore(0);
                                client.getConnectionStateListenable().addListener
                                (
                                    new ConnectionStateListener()
                                    {
                                        @Override
                                        public void stateChanged(CuratorFramework client, ConnectionState newState)
                                        {
                                            if ( (newState == ConnectionState.SUSPENDED) || (newState == ConnectionState.LOST) )
                                            {
                                                suspendedLatch.countDown();
                                                suspendedSemaphore.release();
                                            }
                                        }
                                    }
                                );

                                client.start();

                                InterProcessSemaphoreV2   semaphore = new InterProcessSemaphoreV2(client, PATH, 1);

                                while ( !Thread.currentThread().isInterrupted() )
                                {
                                    Lease   lease = null;
                                    try
                                    {
                                        lease = semaphore.acquire();
                                        acquiredSemaphore.release();
                                        acquireCount.incrementAndGet();
                                        suspendedSemaphore.acquire();
                                    }
                                    catch ( Exception e )
                                    {
                                        // just retry
                                    }
                                    finally
                                    {
                                        if ( lease != null )
                                        {
                                            acquireCount.decrementAndGet();
                                            CloseableUtils.closeQuietly(lease);
                                        }
                                    }
                                }
                            }
                            finally
                            {
                                TestCleanState.closeAndTestClean(client);
                            }
                            return null;
                        }
                    }
                );
            }

            assertTrue(timing.acquireSemaphore(acquiredSemaphore));
            assertEquals(1, acquireCount.get());

            cluster.close();
            timing.awaitLatch(suspendedLatch);
            timing.forWaiting().sleepABit();
            assertEquals(0, acquireCount.get());

            cluster = createAndStartCluster(3);

            connectionString.set(cluster.getConnectString());
            timing.forWaiting().sleepABit();

            assertTrue(timing.acquireSemaphore(acquiredSemaphore));
            timing.forWaiting().sleepABit();
            assertEquals(1, acquireCount.get());
        }
        finally
        {
            executorService.shutdown();
            executorService.awaitTermination(10, TimeUnit.SECONDS);
            executorService.shutdownNow();
            CloseableUtils.closeQuietly(cluster);
        }
    }

    @RepeatedIfExceptionsTest(repeats = BaseClassForTests.REPEATS)
    public void     testCluster() throws Exception
    {
        final int           QTY = 20;
        final int           OPERATION_TIME_MS = 1000;
        final String        PATH = "/foo/bar/lock";

        ExecutorService                 executorService = Executors.newFixedThreadPool(QTY);
        ExecutorCompletionService<Void> completionService = new ExecutorCompletionService<Void>(executorService);
        final Timing                    timing = new Timing();
        List<SemaphoreClient>           semaphoreClients = Lists.newArrayList();
        TestingCluster                  cluster = createAndStartCluster(3);
        try
        {
            final AtomicInteger         opCount = new AtomicInteger(0);
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

            assertNotNull(SemaphoreClient.getActiveClient());

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
            try
            {
                client.getZookeeperClient().blockUntilConnectedOrTimedOut();

                cluster.stop();

                latch.await();
            }
            finally
            {
                CloseableUtils.closeQuietly(client);
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
                assertTrue((System.currentTimeMillis() - startTicks) < timing.forWaiting().milliseconds());
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
                assertTrue((System.currentTimeMillis() - startTicks) < timing.forWaiting().milliseconds());
            }
        }
        finally
        {
            for ( SemaphoreClient semaphoreClient : semaphoreClients )
            {
                CloseableUtils.closeQuietly(semaphoreClient);
            }
            CloseableUtils.closeQuietly(cluster);
            executorService.shutdownNow();
        }
    }
}
