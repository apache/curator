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

package com.netflix.curator.test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Utility to get various testing times
 */
public class Timing
{
    private final long      value;
    private final TimeUnit  unit;

    private static final int DEFAULT_SECONDS = 5;

    /**
     * Use the default base time
     */
    public Timing()
    {
        value = DEFAULT_SECONDS;
        unit = TimeUnit.SECONDS;
    }

    /**
     * Use a multiple of the default base time
     *
     * @param multiple the multiple
     */
    public Timing(int multiple)
    {
        value = DEFAULT_SECONDS * multiple;
        unit = TimeUnit.SECONDS;
    }

    /**
     * @param value base time
     * @param unit base time unit
     */
    public Timing(long value, TimeUnit unit)
    {
        this.value = value;
        this.unit = unit;
    }

    /**
     * Return the base time in milliseconds
     * 
     * @return time ms
     */
    public int  milliseconds()
    {
        return (int)TimeUnit.MILLISECONDS.convert(value, unit);
    }

    /**
     * Return the base time in seconds
     * 
     * @return time secs
     */
    public int seconds()
    {
        return (int)value;
    }

    /**
     * Wait on the given latch
     * 
     * @param latch latch to wait on
     * @return result of {@link CountDownLatch#await(long, TimeUnit)}
     */
    public boolean awaitLatch(CountDownLatch latch)
    {
        Timing m = waitingMultiple();
        try
        {
            return latch.await(m.value, m.unit);
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
        }
        return false;
    }

    /**
     * Wait on the given semaphore
     *
     * @param semaphore the semaphore
     * @return result of {@link Semaphore#tryAcquire()}
     */
    public boolean acquireSemaphore(Semaphore semaphore)
    {
        Timing m = waitingMultiple();
        try
        {
            return semaphore.tryAcquire(m.value, m.unit);
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
        }
        return false;
    }

    /**
     * Wait on the given semaphore
     *
     * @param semaphore the semaphore
     * @param n number of permits to acquire
     * @return result of {@link Semaphore#tryAcquire(int, long, TimeUnit)}
     */
    public boolean acquireSemaphore(Semaphore semaphore, int n)
    {
        Timing m = waitingMultiple();
        try
        {
            return semaphore.tryAcquire(n, m.value, m.unit);
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
        }
        return false;
    }

    /**
     * Return a new timing that is a multiple of the this timing
     *
     * @param n the multiple
     * @return this timing times the multiple
     */
    public Timing   multiple(int n)
    {
        return new Timing(value * n, unit);
    }

    /**
     * Return a new timing with the standard multiple for waiting on latches, etc.
     *
     * @return this timing multiplied
     */
    public Timing   waitingMultiple()
    {
        return multiple(4);
    }

    /**
     * Sleep for a small amount of time
     *
     * @throws InterruptedException if interrupted
     */
    public void sleepABit() throws InterruptedException
    {
        unit.sleep(value / 4);
    }

    /**
     * Return the value to use for ZK session timeout
     * @return session timeout
     */
    public int  session()
    {
        return multiple(10).milliseconds();
    }

    /**
     * Return the value to use for ZK connection timeout
     * @return connection timeout
     */
    public int  connection()
    {
        return milliseconds();
    }
}
