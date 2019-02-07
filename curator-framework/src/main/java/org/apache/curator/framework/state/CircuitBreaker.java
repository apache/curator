package org.apache.curator.framework.state;

import org.apache.curator.RetryPolicy;
import org.apache.curator.RetrySleeper;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

// must be guarded by sync
class CircuitBreaker
{
    private final RetryPolicy retryPolicy;
    private final ScheduledExecutorService service;

    private boolean isOpen = false;
    private int retryCount = 0;
    private long openStartNanos = 0;

    CircuitBreaker(RetryPolicy retryPolicy, ScheduledExecutorService service)
    {
        this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy cannot be null");
        this.service = Objects.requireNonNull(service, "service cannot be null");
    }

    boolean isOpen()
    {
        return isOpen;
    }

    int getRetryCount()
    {
        return retryCount;
    }

    boolean tryToOpen(Runnable completion)
    {
        if ( isOpen )
        {
            return false;
        }

        isOpen = true;
        retryCount = 0;
        openStartNanos = System.nanoTime();
        if ( !tryToRetry(completion) )
        {
            close();
            return false;
        }
        return true;
    }

    boolean tryToRetry(Runnable completion)
    {
        if ( !isOpen )
        {
            return false;
        }

        long[] sleepTimeNanos = new long[]{0L};
        RetrySleeper retrySleeper = (time, unit) -> sleepTimeNanos[0] = unit.toNanos(time);
        if ( !retryPolicy.allowRetry(retryCount, System.nanoTime() - openStartNanos, retrySleeper) )
        {
            return false;
        }
        ++retryCount;
        service.schedule(completion, sleepTimeNanos[0], TimeUnit.NANOSECONDS);
        return true;
    }

    boolean close()
    {
        boolean wasOpen = isOpen;
        retryCount = 0;
        isOpen = false;
        openStartNanos = 0;
        return wasOpen;
    }
}
