package org.apache.curator.utils;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestCloseableScheduledExecutorService
{
    private static final int QTY = 10;
    private static final int DELAY_MS = 100;

    private volatile ScheduledExecutorService executorService;

    @BeforeMethod
    public void setup()
    {
        executorService = Executors.newScheduledThreadPool(QTY * 2);
    }

    @AfterMethod
    public void tearDown()
    {
        executorService.shutdownNow();
    }

    @Test
    public void testCloseableScheduleWithFixedDelay() throws InterruptedException
    {
        CloseableScheduledExecutorService service = new CloseableScheduledExecutorService(executorService);

        final CountDownLatch latch = new CountDownLatch(QTY);
        service.scheduleWithFixedDelay(
                new Runnable()
                {
                    @Override
                    public void run()
                    {
                        latch.countDown();
                    }
                },
                DELAY_MS,
                DELAY_MS,
                TimeUnit.MILLISECONDS
        );

        Assert.assertTrue(latch.await((QTY * 2) * DELAY_MS, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testCloseableScheduleWithFixedDelayAndAdditionalTasks() throws InterruptedException
    {
        final AtomicInteger outerCounter = new AtomicInteger(0);
        Runnable command = new Runnable()
        {
            @Override
            public void run()
            {
                outerCounter.incrementAndGet();
            }
        };
        executorService.scheduleWithFixedDelay(command, DELAY_MS, DELAY_MS, TimeUnit.MILLISECONDS);

        CloseableScheduledExecutorService service = new CloseableScheduledExecutorService(executorService);

        final AtomicInteger innerCounter = new AtomicInteger(0);
        service.scheduleWithFixedDelay(new Runnable()
        {
            @Override
            public void run()
            {
                innerCounter.incrementAndGet();
            }
        }, DELAY_MS, DELAY_MS, TimeUnit.MILLISECONDS);

        Thread.sleep(DELAY_MS * 4);

        service.close();
        Thread.sleep(DELAY_MS * 2);

        int innerValue = innerCounter.get();
        Assert.assertTrue(innerValue > 0);

        int value = outerCounter.get();
        Thread.sleep(DELAY_MS * 2);
        int newValue = outerCounter.get();
        Assert.assertTrue(newValue > value);
        Assert.assertEquals(innerValue, innerCounter.get());

        value = newValue;
        Thread.sleep(DELAY_MS * 2);
        newValue = outerCounter.get();
        Assert.assertTrue(newValue > value);
        Assert.assertEquals(innerValue, innerCounter.get());
    }
}
