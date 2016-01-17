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

package org.apache.curator.utils;

import com.google.common.collect.Lists;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class TestCloseableExecutorService
{
    private static final int QTY = 10;

    private volatile ExecutorService executorService;

    @BeforeMethod
    public void setup()
    {
        executorService = Executors.newFixedThreadPool(QTY * 2);
    }

    @AfterMethod
    public void tearDown()
    {
        executorService.shutdownNow();
    }

    @Test
    public void testBasicRunnable() throws InterruptedException
    {
        CloseableExecutorService service = new CloseableExecutorService(executorService);
        CountDownLatch startLatch = new CountDownLatch(QTY);
        CountDownLatch latch = new CountDownLatch(QTY);
        for ( int i = 0; i < QTY; ++i )
        {
            submitRunnable(service, startLatch, latch);
        }

        Assert.assertTrue(startLatch.await(3, TimeUnit.SECONDS));
        service.close();
        Assert.assertTrue(latch.await(3, TimeUnit.SECONDS));
    }

    @Test
    public void testBasicCallable() throws InterruptedException
    {
        CloseableExecutorService service = new CloseableExecutorService(executorService);
        final CountDownLatch startLatch = new CountDownLatch(QTY);
        final CountDownLatch latch = new CountDownLatch(QTY);
        for ( int i = 0; i < QTY; ++i )
        {
            service.submit
            (
                new Callable<Void>()
                {
                    @Override
                    public Void call() throws Exception
                    {
                        try
                        {
                            startLatch.countDown();
                            Thread.currentThread().join();
                        }
                        catch ( InterruptedException e )
                        {
                            Thread.currentThread().interrupt();
                        }
                        finally
                        {
                            latch.countDown();
                        }
                        return null;
                    }
                }
            );
        }

        Assert.assertTrue(startLatch.await(3, TimeUnit.SECONDS));
        service.close();
        Assert.assertTrue(latch.await(3, TimeUnit.SECONDS));
    }

    @Test
    public void testListeningRunnable() throws InterruptedException
    {
        CloseableExecutorService service = new CloseableExecutorService(executorService);
        List<Future<?>> futures = Lists.newArrayList();
        final CountDownLatch startLatch = new CountDownLatch(QTY);
        for ( int i = 0; i < QTY; ++i )
        {
            Future<?> future = service.submit
            (
                new Runnable()
                {
                    @Override
                    public void run()
                    {
                        try
                        {
                            startLatch.countDown();
                            Thread.currentThread().join();
                        }
                        catch ( InterruptedException e )
                        {
                            Thread.currentThread().interrupt();
                        }
                    }
                }
            );
            futures.add(future);
        }

        Assert.assertTrue(startLatch.await(3, TimeUnit.SECONDS));

        for ( Future<?> future : futures )
        {
            future.cancel(true);
        }

        Assert.assertEquals(service.size(), 0);
    }

    @Test
    public void testListeningCallable() throws InterruptedException
    {
        CloseableExecutorService service = new CloseableExecutorService(executorService);
        final CountDownLatch startLatch = new CountDownLatch(QTY);
        List<Future<?>> futures = Lists.newArrayList();
        for ( int i = 0; i < QTY; ++i )
        {
            Future<?> future = service.submit
            (
                new Callable<Void>()
                {
                    @Override
                    public Void call() throws Exception
                    {
                        try
                        {
                            startLatch.countDown();
                            Thread.currentThread().join();
                        }
                        catch ( InterruptedException e )
                        {
                            Thread.currentThread().interrupt();
                        }
                        return null;
                    }
                }
            );
            futures.add(future);
        }

        Assert.assertTrue(startLatch.await(3, TimeUnit.SECONDS));
        for ( Future<?> future : futures )
        {
            future.cancel(true);
        }

        Assert.assertEquals(service.size(), 0);
    }

    @Test
    public void testPartialRunnable() throws InterruptedException
    {
        final CountDownLatch outsideLatch = new CountDownLatch(1);
        executorService.submit
        (
            new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        Thread.currentThread().join();
                    }
                    catch ( InterruptedException e )
                    {
                        Thread.currentThread().interrupt();
                    }
                    finally
                    {
                        outsideLatch.countDown();
                    }
                }
            }
        );

        CloseableExecutorService service = new CloseableExecutorService(executorService);
        CountDownLatch startLatch = new CountDownLatch(QTY);
        CountDownLatch latch = new CountDownLatch(QTY);
        for ( int i = 0; i < QTY; ++i )
        {
            submitRunnable(service, startLatch, latch);
        }

        while ( service.size() < QTY )
        {
            Thread.sleep(100);
        }

        Assert.assertTrue(startLatch.await(3, TimeUnit.SECONDS));
        service.close();
        Assert.assertTrue(latch.await(3, TimeUnit.SECONDS));
        Assert.assertEquals(outsideLatch.getCount(), 1);
    }

    private void submitRunnable(CloseableExecutorService service, final CountDownLatch startLatch, final CountDownLatch latch)
    {
        service.submit
            (
                new Runnable()
                {
                    @Override
                    public void run()
                    {
                        try
                        {
                            startLatch.countDown();
                            Thread.sleep(100000);
                        }
                        catch ( InterruptedException e )
                        {
                            Thread.currentThread().interrupt();
                        }
                        finally
                        {
                            latch.countDown();
                        }
                    }
                }
            );
    }
}
