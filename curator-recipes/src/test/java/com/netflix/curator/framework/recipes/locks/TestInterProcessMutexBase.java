/*
 *
 *  Copyright 2011 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.curator.framework.recipes.locks;

import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.recipes.BaseClassForTests;
import com.netflix.curator.framework.recipes.KillSessionAndWait;
import com.netflix.curator.retry.RetryOneTime;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public abstract class TestInterProcessMutexBase extends BaseClassForTests
{
    private volatile CountDownLatch         waitLatchForBar = null;
    private volatile CountDownLatch         countLatchForBar = null;

    protected abstract InterProcessLock      makeLock(CuratorFramework client);

    @Test
    public void     testKilledSession() throws Exception
    {
        final int TIMEOUT_SECONDS = 5000;

        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), TIMEOUT_SECONDS * 1000, TIMEOUT_SECONDS * 1000, new RetryOneTime(1));
        client.start();
        try
        {
            final InterProcessLock mutex1 = makeLock(client);
            final InterProcessLock mutex2 = makeLock(client);

            final Semaphore acquireSemaphore = new Semaphore(0);
            Executors.newSingleThreadExecutor().submit
            (
                new Callable<Object>()
                {
                    @Override
                    public Object call() throws Exception
                    {
                        mutex1.acquire();
                        acquireSemaphore.release();
                        Thread.sleep(1000000);
                        return null;
                    }
                }
            );

            Executors.newSingleThreadExecutor().submit
            (
                new Callable<Object>()
                {
                    @Override
                    public Object call() throws Exception
                    {
                        mutex2.acquire();
                        acquireSemaphore.release();
                        Thread.sleep(1000000);
                        return null;
                    }
                }
            );

            Assert.assertTrue(acquireSemaphore.tryAcquire(1, TIMEOUT_SECONDS * 2, TimeUnit.SECONDS));
            KillSessionAndWait.kill(client, server.getConnectString());
            Assert.assertTrue(acquireSemaphore.tryAcquire(1, TIMEOUT_SECONDS * 2, TimeUnit.SECONDS));
        }
        finally
        {
            client.close();
        }
    }

    @Test
    public void     testWithNamespace() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.builder().
            connectString(server.getConnectString()).
            retryPolicy(new RetryOneTime(1)).
            namespace("test").
            build();
        client.start();
        try
        {
            InterProcessLock mutex = makeLock(client);
            mutex.acquire();
            Thread.sleep(100);
            mutex.release();
        }
        finally
        {
            client.close();
        }
    }

    @Test
    public void     testReentrant2Threads() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            waitLatchForBar = new CountDownLatch(1);
            countLatchForBar = new CountDownLatch(1);

            final InterProcessLock mutex = makeLock(client);
            Executors.newSingleThreadExecutor().submit
            (
                new Callable<Object>()
                {
                    @Override
                    public Object call() throws Exception
                    {
                        Assert.assertTrue(countLatchForBar.await(10, TimeUnit.SECONDS));
                        try
                        {
                            mutex.acquire();
                            Assert.fail();
                        }
                        catch ( Exception e )
                        {
                            // correct
                        }
                        finally
                        {
                            waitLatchForBar.countDown();
                        }
                        return null;
                    }
                }
            );

            foo(mutex);
            Assert.assertFalse(mutex.isAcquiredInThisProcess());
        }
        finally
        {
            client.close();
        }
    }

    @Test
    public void     testReentrant() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            InterProcessLock mutex = makeLock(client);
            foo(mutex);
            Assert.assertFalse(mutex.isAcquiredInThisProcess());
        }
        finally
        {
            client.close();
        }
    }

    private void        foo(InterProcessLock mutex) throws Exception
    {
        mutex.acquire();
        Assert.assertTrue(mutex.isAcquiredInThisProcess());
        bar(mutex);
        Assert.assertTrue(mutex.isAcquiredInThisProcess());
        mutex.release();
    }

    private void        bar(InterProcessLock mutex) throws Exception
    {
        mutex.acquire();
        Assert.assertTrue(mutex.isAcquiredInThisProcess());
        if ( countLatchForBar != null )
        {
            countLatchForBar.countDown();
            waitLatchForBar.await();
        }
        snafu(mutex);
        Assert.assertTrue(mutex.isAcquiredInThisProcess());
        mutex.release();
    }

    private void        snafu(InterProcessLock mutex) throws Exception
    {
        mutex.acquire();
        Assert.assertTrue(mutex.isAcquiredInThisProcess());
        mutex.release();
        Assert.assertTrue(mutex.isAcquiredInThisProcess());
    }

    @Test
    public void     test2Clients() throws Exception
    {
        CuratorFramework client1 = null;
        CuratorFramework client2 = null;
        try
        {
            client1 = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
            client2 = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
            client1.start();
            client2.start();

            final InterProcessLock mutexForClient1 = makeLock(client1);
            final InterProcessLock mutexForClient2 = makeLock(client2);

            final CountDownLatch              latchForClient1 = new CountDownLatch(1);
            final CountDownLatch              latchForClient2 = new CountDownLatch(1);
            final CountDownLatch              acquiredLatchForClient1 = new CountDownLatch(1);
            final CountDownLatch              acquiredLatchForClient2 = new CountDownLatch(1);

            final AtomicReference<Exception>  exceptionRef = new AtomicReference<Exception>();

            ExecutorService                   service = Executors.newCachedThreadPool();
            Future<Object>                    future1 = service.submit
            (
                new Callable<Object>()
                {
                    @Override
                    public Object call() throws Exception
                    {
                        try
                        {
                            mutexForClient1.acquire();
                            acquiredLatchForClient1.countDown();
                            latchForClient1.await();
                            mutexForClient1.release();
                        }
                        catch ( Exception e )
                        {
                            exceptionRef.set(e);
                        }
                        return null;
                    }
                }
            );
            Future<Object>                    future2 = service.submit
            (
                new Callable<Object>()
                {
                    @Override
                    public Object call() throws Exception
                    {
                        try
                        {
                            mutexForClient2.acquire();
                            acquiredLatchForClient2.countDown();
                            latchForClient2.await();
                            mutexForClient2.release();
                        }
                        catch ( Exception e )
                        {
                            exceptionRef.set(e);
                        }
                        return null;
                    }
                }
            );

            while ( !mutexForClient1.isAcquiredInThisProcess() && !mutexForClient2.isAcquiredInThisProcess() )
            {
                Thread.sleep(1000);
                Assert.assertFalse(future1.isDone() && future2.isDone());
            }

            Assert.assertTrue(mutexForClient1.isAcquiredInThisProcess() != mutexForClient2.isAcquiredInThisProcess());
            Thread.sleep(1000);
            Assert.assertTrue(mutexForClient1.isAcquiredInThisProcess() || mutexForClient2.isAcquiredInThisProcess());
            Assert.assertTrue(mutexForClient1.isAcquiredInThisProcess() != mutexForClient2.isAcquiredInThisProcess());

            Exception exception = exceptionRef.get();
            if ( exception != null )
            {
                throw exception;
            }

            if ( mutexForClient1.isAcquiredInThisProcess() )
            {
                latchForClient1.countDown();
                Assert.assertTrue(acquiredLatchForClient2.await(10, TimeUnit.SECONDS));
                Assert.assertTrue(mutexForClient2.isAcquiredInThisProcess());
            }
            else
            {
                latchForClient2.countDown();
                Assert.assertTrue(acquiredLatchForClient1.await(10, TimeUnit.SECONDS));
                Assert.assertTrue(mutexForClient1.isAcquiredInThisProcess());
            }
        }
        finally
        {
            Closeables.closeQuietly(client1);
            Closeables.closeQuietly(client2);
        }
    }
}
