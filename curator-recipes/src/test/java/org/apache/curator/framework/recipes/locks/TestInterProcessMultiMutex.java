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

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestInterProcessMultiMutex extends TestInterProcessMutexBase
{
    private static final String     LOCK_PATH_1 = LOCK_BASE_PATH + "/our-lock-1";
    private static final String     LOCK_PATH_2 = LOCK_BASE_PATH + "/our-lock-2";

    @Override
    protected InterProcessLock makeLock(CuratorFramework client)
    {
        return new InterProcessMultiLock(client, Arrays.asList(LOCK_PATH_1, LOCK_PATH_2));
    }

    @Test
    public void testSomeReleasesFail() throws IOException
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            InterProcessLock        goodLock = new InterProcessMutex(client, LOCK_PATH_1);
            final InterProcessLock  otherGoodLock = new InterProcessMutex(client, LOCK_PATH_2);
            InterProcessLock        badLock = new InterProcessLock()
            {
                @Override
                public void acquire() throws Exception
                {
                    otherGoodLock.acquire();
                }

                @Override
                public boolean acquire(long time, TimeUnit unit) throws Exception
                {
                    return otherGoodLock.acquire(time, unit);
                }

                @Override
                public void release() throws Exception
                {
                    throw new Exception("foo");
                }

                @Override
                public boolean isAcquiredInThisProcess()
                {
                    return otherGoodLock.isAcquiredInThisProcess();
                }
            };

            InterProcessMultiLock       lock = new InterProcessMultiLock(Arrays.asList(goodLock, badLock));
            try
            {
                lock.acquire();
                lock.release();
                Assert.fail();
            }
            catch ( Exception e )
            {
            }
            Assert.assertFalse(goodLock.isAcquiredInThisProcess());
            Assert.assertTrue(otherGoodLock.isAcquiredInThisProcess());
        }
        finally
        {
            client.close();
        }
    }

    @Test
    public void testSomeLocksFailToLock() throws IOException
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            final AtomicBoolean     goodLockWasLocked = new AtomicBoolean(false);
            final InterProcessLock  goodLock = new InterProcessMutex(client, LOCK_PATH_1);
            InterProcessLock        badLock = new InterProcessLock()
            {
                @Override
                public void acquire() throws Exception
                {
                    if ( goodLock.isAcquiredInThisProcess() )
                    {
                        goodLockWasLocked.set(true);
                    }
                    throw new Exception("foo");
                }

                @Override
                public boolean acquire(long time, TimeUnit unit) throws Exception
                {
                    throw new Exception("foo");
                }

                @Override
                public void release() throws Exception
                {
                    throw new Exception("foo");
                }

                @Override
                public boolean isAcquiredInThisProcess()
                {
                    return false;
                }
            };

            InterProcessMultiLock       lock = new InterProcessMultiLock(Arrays.asList(goodLock, badLock));
            try
            {
                lock.acquire();
                Assert.fail();
            }
            catch ( Exception e )
            {
            }
            Assert.assertFalse(goodLock.isAcquiredInThisProcess());
            Assert.assertTrue(goodLockWasLocked.get());
        }
        finally
        {
            client.close();
        }
    }
}
