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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.github.artsok.RepeatedIfExceptionsTest;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.CuratorTempFramework;
import org.apache.curator.retry.RetryOneTime;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TestTempFramework extends BaseClassForTests
{
    @RepeatedIfExceptionsTest(repeats = BaseClassForTests.REPEATS)
    public void testBasic() throws Exception
    {
        CuratorTempFramework        client = CuratorFrameworkFactory.builder().connectString(server.getConnectString()).retryPolicy(new RetryOneTime(1)).buildTemp();
        try
        {
            client.inTransaction().create().forPath("/foo", "data".getBytes()).and().commit();

            byte[] bytes = client.getData().forPath("/foo");
            assertArrayEquals(bytes, "data".getBytes());
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @RepeatedIfExceptionsTest(repeats = BaseClassForTests.REPEATS)
    public void testInactivity() throws Exception
    {
        final CuratorTempFrameworkImpl        client = (CuratorTempFrameworkImpl)CuratorFrameworkFactory.builder().connectString(server.getConnectString()).retryPolicy(new RetryOneTime(1)).buildTemp(1, TimeUnit.SECONDS);
        try
        {
            ScheduledExecutorService    service = Executors.newScheduledThreadPool(1);
            Runnable                    command = new Runnable()
            {
                @Override
                public void run()
                {
                    client.updateLastAccess();
                }
            };
            service.scheduleAtFixedRate(command, 10, 10, TimeUnit.MILLISECONDS);
            client.inTransaction().create().forPath("/foo", "data".getBytes()).and().commit();
            service.shutdownNow();
            Thread.sleep(2000);

            assertNull(client.getCleanup());
            assertNull(client.getClient());
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }
}
