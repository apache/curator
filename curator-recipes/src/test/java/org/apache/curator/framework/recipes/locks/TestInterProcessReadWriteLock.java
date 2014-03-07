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
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class TestInterProcessReadWriteLock extends TestInterProcessReadWriteLockBase
{
    @Test
    public void testGetParticipantNodes() throws Exception
    {
        final int READERS = 20;
        final int WRITERS = 8;

        final CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();

            final CountDownLatch latch = new CountDownLatch(READERS + WRITERS);
            final CountDownLatch readLatch = new CountDownLatch(READERS);

            ExecutorService service = Executors.newCachedThreadPool();
            for ( int i = 0; i < READERS; ++i )
            {
                service.submit(new Callable<Void>()
                {
                    @Override
                    public Void call() throws Exception
                    {
                        InterProcessReadWriteLockBase lock = newLock(client, "/lock");
                        lock.readLock().acquire();
                        latch.countDown();
                        readLatch.countDown();
                        return null;
                    }
                });
            }
            for ( int i = 0; i < WRITERS; ++i )
            {
                service.submit(new Callable<Void>()
                {
                    @Override
                    public Void call() throws Exception
                    {
                        InterProcessReadWriteLockBase lock = newLock(client, "/lock");
                        Assert.assertTrue(readLatch.await(10, TimeUnit.SECONDS));
                        latch.countDown();  // must be before as there can only be one writer
                        lock.writeLock().acquire();
                        return null;
                    }
                });
            }

            Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
            new Timing().sleepABit();

            InterProcessReadWriteLockBase lock = newLock(client, "/lock");
            Collection<String> readers = lock.readLock().getParticipantNodes();
            Collection<String> writers = lock.writeLock().getParticipantNodes();

            Assert.assertEquals(readers.size(), READERS);
            Assert.assertEquals(writers.size(), WRITERS);
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testThatUpgradingIsDisallowed() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();

            InterProcessReadWriteLockBase lock = newLock(client, "/lock");
            lock.readLock().acquire();
            Assert.assertFalse(lock.writeLock().acquire(5, TimeUnit.SECONDS));

            lock.readLock().release();
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testThatDowngradingRespectsThreads() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();

            final InterProcessReadWriteLockBase lock = newLock(client, "/lock");
            ExecutorService t1 = Executors.newSingleThreadExecutor();
            ExecutorService t2 = Executors.newSingleThreadExecutor();

            final CountDownLatch latch = new CountDownLatch(1);

            Future<Object> f1 = t1.submit(new Callable<Object>()
            {
                @Override
                public Object call() throws Exception
                {
                    lock.writeLock().acquire();
                    latch.countDown();
                    return null;
                }
            });

            Future<Object> f2 = t2.submit(new Callable<Object>()
            {
                @Override
                public Object call() throws Exception
                {
                    Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
                    Assert.assertFalse(lock.readLock().acquire(5, TimeUnit.SECONDS));
                    return null;
                }
            });

            f1.get();
            f2.get();
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testDowngrading() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();

            InterProcessReadWriteLockBase lock = newLock(client, "/lock");
            lock.writeLock().acquire();
            Assert.assertTrue(lock.readLock().acquire(5, TimeUnit.SECONDS));
            lock.writeLock().release();

            lock.readLock().release();
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Override
    protected InterProcessReadWriteLockBase newLock(CuratorFramework client, String path)
    {
        return new InterProcessReadWriteLock(client, path);
    }
}
