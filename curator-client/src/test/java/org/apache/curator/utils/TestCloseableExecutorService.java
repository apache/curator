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
import java.util.concurrent.atomic.AtomicInteger;

public class TestCloseableExecutorService
{
    private static final int QTY = 10;

    private volatile ExecutorService executorService;
    private volatile AtomicInteger count;

    @BeforeMethod
    public void setup()
    {
        executorService = Executors.newFixedThreadPool(QTY * 2);
        count = new AtomicInteger(0);
    }

    @AfterMethod
    public void tearDown()
    {
        executorService.shutdownNow();
    }

    @Test
    public void testBasicRunnable() throws InterruptedException
    {
        try
        {
            FutureContainer service = new FutureContainer(executorService);
            CountDownLatch startLatch = new CountDownLatch(QTY);
            CountDownLatch latch = new CountDownLatch(QTY);
            for ( int i = 0; i < QTY; ++i )
            {
                submitRunnable(service, startLatch, latch);
            }

            Assert.assertTrue(startLatch.await(3, TimeUnit.SECONDS), "Latch = " + latch.getCount() + " Count = " + count.get() + " - Size = " + service.size());
            service.close();
            Assert.assertTrue(latch.await(3, TimeUnit.SECONDS), "Latch = " + latch.getCount() + " Count = " + count.get() + " - Size = " + service.size());
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
        List<CountDownLatch> latches = Lists.newArrayList();
        for ( int i = 0; i < QTY; ++i )
        {
            final CountDownLatch latch = new CountDownLatch(1);
            latches.add(latch);
            service.submit
                (
                    new Callable<Void>()
                    {
                        @Override
                        public Void call() throws Exception
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
                                latch.countDown();
                            }
                            return null;
                        }
                    }
                );
        }

        service.close();
        for ( CountDownLatch latch : latches )
        {
            Assert.assertTrue(latch.await(3, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testListeningRunnable() throws InterruptedException
    {
        CloseableExecutorService service = new CloseableExecutorService(executorService);
        List<Future<?>> futures = Lists.newArrayList();
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

        Thread.sleep(100);

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

        Thread.sleep(100);

        for ( Future<?> future : futures )
        {
            future.cancel(true);
        }

        Assert.assertEquals(service.size(), 0);
    }

    @Test
    public void testPartialRunnable() throws InterruptedException
    {
        try
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

            FutureContainer service = new FutureContainer(executorService);
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

            Assert.assertTrue(startLatch.await(3, TimeUnit.SECONDS), "Latch = " + latch.getCount() + " Count = " + count.get() + " - Size = " + service.size());
            service.close();
            Assert.assertTrue(latch.await(3, TimeUnit.SECONDS), "Latch = " + latch.getCount() + " Count = " + count.get() + " - Size = " + service.size());
            Assert.assertEquals(outsideLatch.getCount(), 1);
        }
        catch ( AssertionError e )
        {
            throw e;
        }
        catch ( Throwable e )
        {
            e.printStackTrace();
        }
        finally
        {
            executorService.shutdownNow();
        }
    }

    private void submitRunnable(FutureContainer service, final CountDownLatch startLatch, final CountDownLatch latch)
    {
        try
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
                            count.incrementAndGet();
                            Thread.sleep(100000);
                        }
                        catch ( InterruptedException e )
                        {
                            Thread.currentThread().interrupt();
                        }
                        catch ( Throwable e )
                        {
                            e.printStackTrace();
                        }
                        finally
                        {
    //                        count.decrementAndGet();
                            latch.countDown();
                        }
                    }
                }
            );
        }
        catch ( Throwable e )
        {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }
}
