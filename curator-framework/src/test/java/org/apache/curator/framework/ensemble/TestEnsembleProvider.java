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
package org.apache.curator.framework.ensemble;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.artsok.RepeatedIfExceptionsTest;
import org.apache.curator.ensemble.EnsembleProvider;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.TestingServer;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;

public class TestEnsembleProvider extends BaseClassForTests
{
    private final Timing timing = new Timing();

    @RepeatedIfExceptionsTest(repeats = BaseClassForTests.REPEATS)
    public void testBasic()
    {
        Semaphore counter = new Semaphore(0);
        final CuratorFramework client = newClient(counter);
        try
        {
            client.start();
            assertTrue(timing.acquireSemaphore(counter));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @RepeatedIfExceptionsTest(repeats = BaseClassForTests.REPEATS)
    public void testAfterSessionExpiration() throws Exception
    {
        TestingServer oldServer = server;
        Semaphore counter = new Semaphore(0);
        final CuratorFramework client = newClient(counter);
        try
        {
            final CountDownLatch connectedLatch = new CountDownLatch(1);
            final CountDownLatch lostLatch = new CountDownLatch(1);
            final CountDownLatch reconnectedLatch = new CountDownLatch(1);
            ConnectionStateListener listener = new ConnectionStateListener()
            {
                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState)
                {
                    if ( newState == ConnectionState.CONNECTED )
                    {
                        connectedLatch.countDown();
                    }
                    if ( newState == ConnectionState.LOST )
                    {
                        lostLatch.countDown();
                    }
                    if ( newState == ConnectionState.RECONNECTED )
                    {
                        reconnectedLatch.countDown();
                    }
                }
            };
            client.getConnectionStateListenable().addListener(listener);
            client.start();

            assertTrue(timing.awaitLatch(connectedLatch));

            server.stop();

            assertTrue(timing.awaitLatch(lostLatch));
            counter.drainPermits();
            for ( int i = 0; i < 5; ++i )
            {
                // the ensemble provider should still be called periodically when the connection is lost
                assertTrue(timing.acquireSemaphore(counter), "Failed when i is: " + i);
            }

            server = new TestingServer();   // this changes the CountingEnsembleProvider's value for getConnectionString() - connection should notice this and recover
            assertTrue(timing.awaitLatch(reconnectedLatch));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
            CloseableUtils.closeQuietly(oldServer);
        }
    }

    private CuratorFramework newClient(Semaphore counter)
    {
        return CuratorFrameworkFactory.builder()
            .ensembleProvider(new CountingEnsembleProvider(counter))
            .sessionTimeoutMs(timing.session())
            .connectionTimeoutMs(timing.connection())
            .retryPolicy(new RetryOneTime(1))
            .build();
    }

    private class CountingEnsembleProvider implements EnsembleProvider
    {
        private final Semaphore getConnectionStringCounter;

        public CountingEnsembleProvider(Semaphore getConnectionStringCounter)
        {
            this.getConnectionStringCounter = getConnectionStringCounter;
        }

        @Override
        public void start()
        {
            // NOP
        }

        @Override
        public String getConnectionString()
        {
            getConnectionStringCounter.release();
            return server.getConnectString();
        }

        @Override
        public void close()
        {
            // NOP
        }

        @Override
        public void setConnectionString(String connectionString)
        {
            // NOP
        }

        @Override
        public boolean updateServerListEnabled()
        {
            return false;
        }
    }
}
