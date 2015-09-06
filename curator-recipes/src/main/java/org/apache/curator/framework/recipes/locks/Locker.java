package org.apache.curator.framework.recipes.locks;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * <p>
 *     Utility for safely acquiring a lock and releasing it using Java 7's
 *     try-with-resource feature.
 * </p>
 *
 * <p>
 *     Canonical usage:
 * <code><pre>
 *     InterProcessMutex mutex = new InterProcessMutex(...) // or any InterProcessLock
 *     try ( Locker locker = new Locker(mutex, maxTimeout, unit) )
 *     {
 *         // do work
 *     }
 * </pre></code>
 * </p>
 */
public class Locker implements AutoCloseable
{
    private final InterProcessLock lock;
    private final AtomicBoolean acquired;

    /**
     * @param lock a lock implementation (e.g. {@link InterProcessMutex}, {@link InterProcessSemaphoreV2}, etc.)
     * @param timeout max timeout to acquire lock
     * @param unit time unit of timeout
     * @throws Exception Curator errors or {@link TimeoutException} if the lock cannot be acquired within the timeout
     */
    public Locker(InterProcessLock lock, long timeout, TimeUnit unit) throws Exception
    {
        this.lock = lock;
        acquired = new AtomicBoolean(acquireLock(lock, timeout, unit));
        if ( !acquired.get() )
        {
            throw new TimeoutException("Could not acquire lock within timeout of " + unit.toMillis(timeout) + "ms");
        }
    }

    @Override
    /**
     * Relase the lock if it has been acquired. Can be safely called multiple times.
     * Only the first call will unlock.
     */
    public void close() throws Exception
    {
        if ( acquired.compareAndSet(true, false) )
        {
            releaseLock();
        }
    }

    protected void releaseLock() throws Exception
    {
        lock.release();
    }

    protected boolean acquireLock(InterProcessLock lock, long timeout, TimeUnit unit) throws Exception
    {
        return lock.acquire(timeout, unit);
    }
}
