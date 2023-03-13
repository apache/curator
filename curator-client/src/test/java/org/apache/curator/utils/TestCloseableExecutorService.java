/*
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.google.common.collect.Lists;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

    @BeforeEach
    public void setup()
    {
        executorService = Executors.newFixedThreadPool(QTY * 2);
    }

    @AfterEach
    public void tearDown()
    {
        executorService.shutdownNow();
    }

    @Test
    public void testBasicRunnable() throws InterruptedException
    {
        try
        {
            CloseableExecutorService service = new CloseableExecutorService(executorService);
            CountDownLatch startLatch = new CountDownLatch(QTY);
            CountDownLatch latch = new CountDownLatch(QTY);
            for ( int i = 0; i < QTY; ++i )
            {
                submitRunnable(service, startLatch, latch);
            }

            assertTrue(startLatch.await(3, TimeUnit.SECONDS));
            service.close();
            assertTrue(latch.await(3, TimeUnit.SECONDS));
        }
        catch ( AssertionError e )
        {
            throw e;
        }
        catch ( Throwable e )
        {
            e.printStackTrace();
        }
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
                    (Callable<Void>) () -> {
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
            );
        }

        assertTrue(startLatch.await(3, TimeUnit.SECONDS));
        service.close();
        assertTrue(latch.await(3, TimeUnit.SECONDS));
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
                    () -> {
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
            );
            futures.add(future);
        }

        assertTrue(startLatch.await(3, TimeUnit.SECONDS));

        for ( Future<?> future : futures )
        {
            future.cancel(true);
        }

        assertEquals(service.size(), 0);
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
                    (Callable<Void>) () -> {
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
            );
            futures.add(future);
        }

        assertTrue(startLatch.await(3, TimeUnit.SECONDS));
        for ( Future<?> future : futures )
        {
            future.cancel(true);
        }

        assertEquals(service.size(), 0);
    }

    @Test
    public void testPartialRunnable() throws InterruptedException
    {
        final CountDownLatch outsideLatch = new CountDownLatch(1);
        executorService.submit
        (
                () -> {
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
        );

        CloseableExecutorService service = new CloseableExecutorService(executorService);
        CountDownLatch startLatch = new CountDownLatch(QTY);
        CountDownLatch latch = new CountDownLatch(QTY);
        for ( int i = 0; i < QTY; ++i )
        {
            submitRunnable(service, startLatch, latch);
        }

        Awaitility.await().until(()-> service.size() >= QTY);

        assertTrue(startLatch.await(3, TimeUnit.SECONDS));
        service.close();
        assertTrue(latch.await(3, TimeUnit.SECONDS));
        assertEquals(outsideLatch.getCount(), 1);
    }

    private void submitRunnable(CloseableExecutorService service, final CountDownLatch startLatch, final CountDownLatch latch)
    {
        service.submit
            (
                    () -> {
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
            );
    }
}
