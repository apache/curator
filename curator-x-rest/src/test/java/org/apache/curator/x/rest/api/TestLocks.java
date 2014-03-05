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

package org.apache.curator.x.rest.api;

import com.google.common.collect.Lists;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.test.KillSession;
import org.apache.curator.test.TestingServer;
import org.apache.curator.test.Timing;
import org.apache.curator.x.rest.entities.Status;
import org.apache.curator.x.rest.entities.StatusMessage;
import org.apache.curator.x.rest.support.BaseClassForTests;
import org.apache.curator.x.rest.support.InterProcessLockBridge;
import org.apache.curator.x.rest.support.InterProcessReadWriteLockBridge;
import org.apache.curator.x.rest.support.StatusListener;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class TestLocks extends BaseClassForTests
{
    @Test
    public void test2Clients() throws Exception
    {
        final InterProcessLock mutexForClient1 = new InterProcessLockBridge(restClient, sessionManager, uriMaker, "/lock");
        final InterProcessLock mutexForClient2 = new InterProcessLockBridge(restClient, sessionManager, uriMaker, "/lock");

        final CountDownLatch latchForClient1 = new CountDownLatch(1);
        final CountDownLatch latchForClient2 = new CountDownLatch(1);
        final CountDownLatch acquiredLatchForClient1 = new CountDownLatch(1);
        final CountDownLatch acquiredLatchForClient2 = new CountDownLatch(1);

        final AtomicReference<Exception> exceptionRef = new AtomicReference<Exception>();

        ExecutorService service = Executors.newCachedThreadPool();
        Future<Object> future1 = service.submit
            (
                new Callable<Object>()
                {
                    @Override
                    public Object call() throws Exception
                    {
                        try
                        {
                            if ( !mutexForClient1.acquire(10, TimeUnit.SECONDS) )
                            {
                                throw new Exception("mutexForClient1.acquire timed out");
                            }
                            acquiredLatchForClient1.countDown();
                            if ( !latchForClient1.await(10, TimeUnit.SECONDS) )
                            {
                                throw new Exception("latchForClient1 timed out");
                            }
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
        Future<Object> future2 = service.submit
            (
                new Callable<Object>()
                {
                    @Override
                    public Object call() throws Exception
                    {
                        try
                        {
                            if ( !mutexForClient2.acquire(10, TimeUnit.SECONDS) )
                            {
                                throw new Exception("mutexForClient2.acquire timed out");
                            }
                            acquiredLatchForClient2.countDown();
                            if ( !latchForClient2.await(10, TimeUnit.SECONDS) )
                            {
                                throw new Exception("latchForClient2 timed out");
                            }
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
            Exception exception = exceptionRef.get();
            if ( exception != null )
            {
                throw exception;
            }
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

    @Test
    public void testWaitingProcessKilledServer() throws Exception
    {
        final Timing timing = new Timing();
        final CountDownLatch latch = new CountDownLatch(1);
        StatusListener statusListener = new StatusListener()
        {
            @Override
            public void statusUpdate(List<StatusMessage> messages)
            {
                // NOP
            }

            @Override
            public void errorState(Status status)
            {
                if ( status.getState().equals("lost") )
                {
                    latch.countDown();
                }
            }
        };
        sessionManager.setStatusListener(statusListener);

        final AtomicBoolean isFirst = new AtomicBoolean(true);
        ExecutorCompletionService<Object> service = new ExecutorCompletionService<Object>(Executors.newFixedThreadPool(2));
        for ( int i = 0; i < 2; ++i )
        {
            service.submit
                (
                    new Callable<Object>()
                    {
                        @Override
                        public Object call() throws Exception
                        {
                            InterProcessLock lock = new InterProcessLockBridge(restClient, sessionManager, uriMaker, "/lock");
                            lock.acquire();
                            try
                            {
                                if ( isFirst.compareAndSet(true, false) )
                                {
                                    timing.sleepABit();

                                    server.stop();
                                    Assert.assertTrue(timing.awaitLatch(latch));
                                    server = new TestingServer(server.getPort(), server.getTempDirectory());
                                }
                            }
                            finally
                            {
                                try
                                {
                                    lock.release();
                                }
                                catch ( Exception e )
                                {
                                    // ignore
                                }
                            }
                            return null;
                        }
                    }
                );
        }

        for ( int i = 0; i < 2; ++i )
        {
            service.take().get(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS);
        }
    }

    @Test
    public void testKilledSession() throws Exception
    {
        final Timing timing = new Timing();

        final InterProcessLock mutex1 = new InterProcessLockBridge(restClient, sessionManager, uriMaker, "/lock");
        final InterProcessLock mutex2 = new InterProcessLockBridge(restClient, sessionManager, uriMaker, "/lock");

        final Semaphore semaphore = new Semaphore(0);
        ExecutorCompletionService<Object> service = new ExecutorCompletionService<Object>(Executors.newFixedThreadPool(2));
        service.submit
            (
                new Callable<Object>()
                {
                    @Override
                    public Object call() throws Exception
                    {
                        mutex1.acquire();
                        semaphore.release();
                        Thread.sleep(1000000);
                        return null;
                    }
                }
            );

        service.submit
            (
                new Callable<Object>()
                {
                    @Override
                    public Object call() throws Exception
                    {
                        mutex2.acquire();
                        semaphore.release();
                        Thread.sleep(1000000);
                        return null;
                    }
                }
            );

        Assert.assertTrue(timing.acquireSemaphore(semaphore, 1));
        KillSession.kill(getCuratorRestContext().getClient().getZookeeperClient().getZooKeeper(), server.getConnectString());
        Assert.assertTrue(timing.acquireSemaphore(semaphore, 1));
    }

    @Test
    public void testThreading() throws Exception
    {
        final int THREAD_QTY = 10;

        final AtomicBoolean hasLock = new AtomicBoolean(false);
        final AtomicBoolean isFirst = new AtomicBoolean(true);
        final Semaphore semaphore = new Semaphore(1);

        List<Future<Object>> threads = Lists.newArrayList();
        ExecutorService service = Executors.newCachedThreadPool();
        for ( int i = 0; i < THREAD_QTY; ++i )
        {
            final InterProcessLock mutex = new InterProcessLockBridge(restClient, sessionManager, uriMaker, "/lock");
            Future<Object> t = service.submit
                (
                    new Callable<Object>()
                    {
                        @Override
                        public Object call() throws Exception
                        {
                            semaphore.acquire();
                            mutex.acquire();
                            Assert.assertTrue(hasLock.compareAndSet(false, true));
                            try
                            {
                                if ( isFirst.compareAndSet(true, false) )
                                {
                                    semaphore.release(THREAD_QTY - 1);
                                    while ( semaphore.availablePermits() > 0 )
                                    {
                                        Thread.sleep(100);
                                    }
                                }
                                else
                                {
                                    Thread.sleep(100);
                                }
                            }
                            finally
                            {
                                mutex.release();
                                hasLock.set(false);
                            }
                            return null;
                        }
                    }
                );
            threads.add(t);
        }

        for ( Future<Object> t : threads )
        {
            t.get();
        }
    }

    @Test
    public void     testBasicReadWriteLock() throws Exception
    {
        final int               CONCURRENCY = 8;
        final int               ITERATIONS = 100;

        final Random random = new Random();
        final AtomicInteger concurrentCount = new AtomicInteger(0);
        final AtomicInteger     maxConcurrentCount = new AtomicInteger(0);
        final AtomicInteger     writeCount = new AtomicInteger(0);
        final AtomicInteger     readCount = new AtomicInteger(0);

        List<Future<Void>>  futures = Lists.newArrayList();
        ExecutorService     service = Executors.newCachedThreadPool();
        for ( int i = 0; i < CONCURRENCY; ++i )
        {
            Future<Void>    future = service.submit
                (
                    new Callable<Void>()
                    {
                        @Override
                        public Void call() throws Exception
                        {
                            InterProcessReadWriteLockBridge lock = new InterProcessReadWriteLockBridge(restClient, sessionManager, uriMaker, "/lock");
                            for ( int i = 0; i < ITERATIONS; ++i )
                            {
                                if ( random.nextInt(100) < 10 )
                                {
                                    doLocking(lock.writeLock(), concurrentCount, maxConcurrentCount, random, 1);
                                    writeCount.incrementAndGet();
                                }
                                else
                                {
                                    doLocking(lock.readLock(), concurrentCount, maxConcurrentCount, random, Integer.MAX_VALUE);
                                    readCount.incrementAndGet();
                                }
                            }
                            return null;
                        }
                    }
                );
            futures.add(future);
        }

        for ( Future<Void> future : futures )
        {
            future.get();
        }

        System.out.println("Writes: " + writeCount.get() + " - Reads: " + readCount.get() + " - Max Reads: " + maxConcurrentCount.get());

        Assert.assertTrue(writeCount.get() > 0);
        Assert.assertTrue(readCount.get() > 0);
        Assert.assertTrue(maxConcurrentCount.get() > 1);
    }

    private void doLocking(InterProcessLock lock, AtomicInteger concurrentCount, AtomicInteger maxConcurrentCount, Random random, int maxAllowed) throws Exception
    {
        try
        {
            Assert.assertTrue(lock.acquire(10, TimeUnit.SECONDS));
            int     localConcurrentCount;
            synchronized(this)
            {
                localConcurrentCount = concurrentCount.incrementAndGet();
                if ( localConcurrentCount > maxConcurrentCount.get() )
                {
                    maxConcurrentCount.set(localConcurrentCount);
                }
            }

            Assert.assertTrue(localConcurrentCount <= maxAllowed, "" + localConcurrentCount);

            Thread.sleep(random.nextInt(9) + 1);
        }
        finally
        {
            synchronized(this)
            {
                concurrentCount.decrementAndGet();
                lock.release();
            }
        }
    }
}
