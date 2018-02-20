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
package org.apache.curator.ensemble;

import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.concurrent.Semaphore;

public class TestEnsembleProvider extends BaseClassForTests
{
    private final Timing timing = new Timing();

    @Test
    public void testBasic() throws Exception
    {
        Semaphore counter = new Semaphore(0);
        final CuratorZookeeperClient client = new CuratorZookeeperClient(new CountingEnsembleProvider(counter), timing.session(), timing.connection(), null, new RetryOneTime(2));
        try
        {
            client.start();
            Assert.assertTrue(timing.acquireSemaphore(counter));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testAfterSessionExpiration() throws Exception
    {
        Semaphore counter = new Semaphore(0);
        final CuratorZookeeperClient client = new CuratorZookeeperClient(new CountingEnsembleProvider(counter), timing.session(), timing.connection(), null, new RetryOneTime(2));
        try
        {
            client.start();
            Assert.assertTrue(timing.acquireSemaphore(counter));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
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
