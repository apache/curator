package org.apache.curator.framework.state;

import org.apache.curator.retry.RetryForever;
import org.apache.curator.retry.RetryNTimes;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TestCircuitBreaker
{
    private Duration[] lastDelay = new Duration[]{Duration.ZERO};
    private ScheduledThreadPoolExecutor service = new ScheduledThreadPoolExecutor(1)
    {
        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit)
        {
            lastDelay[0] = Duration.of(unit.toNanos(delay), ChronoUnit.NANOS);
            command.run();
            return null;
        }
    };

    @AfterClass
    public void tearDown()
    {
        service.shutdownNow();
    }

    @Test
    public void testBasic()
    {
        final int retryQty = 1;
        final Duration delay = Duration.ofSeconds(10);

        CircuitBreaker circuitBreaker = new CircuitBreaker(new RetryNTimes(retryQty, (int)delay.toMillis()), service);
        AtomicInteger counter = new AtomicInteger(0);

        Assert.assertTrue(circuitBreaker.tryToOpen(counter::incrementAndGet));
        Assert.assertEquals(lastDelay[0], delay);

        Assert.assertFalse(circuitBreaker.tryToOpen(counter::incrementAndGet));
        Assert.assertEquals(circuitBreaker.getRetryCount(), 1);
        Assert.assertEquals(counter.get(), 1);
        Assert.assertFalse(circuitBreaker.tryToRetry(counter::incrementAndGet));
        Assert.assertEquals(circuitBreaker.getRetryCount(), 1);
        Assert.assertEquals(counter.get(), 1);

        Assert.assertTrue(circuitBreaker.close());
        Assert.assertEquals(circuitBreaker.getRetryCount(), 0);
        Assert.assertFalse(circuitBreaker.close());
    }

    @Test
    public void testVariousOpenRetryFails()
    {
        CircuitBreaker circuitBreaker = new CircuitBreaker(new RetryForever(1), service);
        Assert.assertFalse(circuitBreaker.tryToRetry(() -> {}));
        Assert.assertTrue(circuitBreaker.tryToOpen(() -> {}));
        Assert.assertFalse(circuitBreaker.tryToOpen(() -> {}));
        Assert.assertTrue(circuitBreaker.close());
        Assert.assertFalse(circuitBreaker.close());
    }
}
