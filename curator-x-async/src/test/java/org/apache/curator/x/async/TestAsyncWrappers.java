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
package org.apache.curator.x.async;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.RetryOneTime;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class TestAsyncWrappers extends CompletableBaseClassForTests
{
    @Test
    public void testBasic()
    {
        try ( CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1)) )
        {
            client.start();

            InterProcessMutex lock = new InterProcessMutex(client, "/one/two");
            complete(AsyncWrappers.lockAsync(lock), (__, e) -> {
                assertNull(e);
                AsyncWrappers.release(lock);
            });
        }
    }

    @Test
    public void testContention() throws Exception
    {
        try ( CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1)) )
        {
            client.start();

            InterProcessMutex lock1 = new InterProcessMutex(client, "/one/two");
            InterProcessMutex lock2 = new InterProcessMutex(client, "/one/two");
            CountDownLatch latch = new CountDownLatch(1);
            AsyncWrappers.lockAsync(lock1).thenAccept(__ -> {
                latch.countDown();  // don't release the lock
            });
            assertTrue(timing.awaitLatch(latch));

            CountDownLatch latch2 = new CountDownLatch(1);
            AsyncWrappers.lockAsync(lock2, timing.forSleepingABit().milliseconds(), TimeUnit.MILLISECONDS).exceptionally(e -> {
                if ( e instanceof AsyncWrappers.TimeoutException )
                {
                    latch2.countDown();  // lock should still be held
                }
                return null;
            });
            assertTrue(timing.awaitLatch(latch2));
        }
    }
}
