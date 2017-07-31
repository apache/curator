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

package org.apache.curator.test.compatibility;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Utility to get various testing times.
 *
 * Copied from the old Timing class which is now deprecated. Needed this to support ZK 3.4 compatibility
 */
public class Timing2
{
    private final long value;
    private final TimeUnit unit;
    private final int waitingMultiple;

    private static final int DEFAULT_SECONDS = 10;
    private static final int DEFAULT_WAITING_MULTIPLE = 5;
    private static final double SESSION_MULTIPLE = 1.5;
    private static final double SESSION_SLEEP_MULTIPLE = SESSION_MULTIPLE * 1.75;  // has to be at least session + 2/3 of a session to account for missed heartbeat then session expiration

    /**
     * Use the default base time
     */
    public Timing2()
    {
        this(Integer.getInteger("timing-multiple", 1), getWaitingMultiple());
    }

    /**
     * Use a multiple of the default base time
     *
     * @param multiple the multiple
     */
    public Timing2(double multiple)
    {
        this((long)(DEFAULT_SECONDS * multiple), TimeUnit.SECONDS, getWaitingMultiple());
    }

    /**
     * Use a multiple of the default base time
     *
     * @param multiple the multiple
     * @param waitingMultiple multiple of main timing to use when waiting
     */
    public Timing2(double multiple, int waitingMultiple)
    {
        this((long)(DEFAULT_SECONDS * multiple), TimeUnit.SECONDS, waitingMultiple);
    }

    /**
     * @param value base time
     * @param unit  base time unit
     */
    public Timing2(long value, TimeUnit unit)
    {
        this(value, unit, getWaitingMultiple());
    }

    /**
     * @param value base time
     * @param unit  base time unit
     * @param waitingMultiple multiple of main timing to use when waiting
     */
    public Timing2(long value, TimeUnit unit, int waitingMultiple)
    {
        this.value = value;
        this.unit = unit;
        this.waitingMultiple = waitingMultiple;
    }

    /**
     * Return the base time in milliseconds
     *
     * @return time ms
     */
    public int milliseconds()
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
     * @return result of {@link java.util.concurrent.CountDownLatch#await(long, java.util.concurrent.TimeUnit)}
     */
    public boolean awaitLatch(CountDownLatch latch)
    {
        Timing2 m = forWaiting();
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
     * Try to take an item from the given queue
     *
     * @param queue queue
     * @return item
     * @throws Exception interrupted or timed out
     */
    public <T> T takeFromQueue(BlockingQueue<T> queue) throws Exception
    {
        Timing2 m = forWaiting();
        try
        {
            T value = queue.poll(m.value, m.unit);
            if ( value == null )
            {
                throw new TimeoutException("Timed out trying to take from queue");
            }
            return value;
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            throw e;
        }
    }

    /**
     * Wait on the given semaphore
     *
     * @param semaphore the semaphore
     * @return result of {@link java.util.concurrent.Semaphore#tryAcquire()}
     */
    public boolean acquireSemaphore(Semaphore semaphore)
    {
        Timing2 m = forWaiting();
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
     * @param n         number of permits to acquire
     * @return result of {@link java.util.concurrent.Semaphore#tryAcquire(int, long, java.util.concurrent.TimeUnit)}
     */
    public boolean acquireSemaphore(Semaphore semaphore, int n)
    {
        Timing2 m = forWaiting();
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
    public Timing2 multiple(double n)
    {
        return new Timing2((int)(value * n), unit);
    }

    /**
     * Return a new timing that is a multiple of the this timing
     *
     * @param n the multiple
     * @param waitingMultiple new waitingMultiple
     * @return this timing times the multiple
     */
    public Timing2 multiple(double n, int waitingMultiple)
    {
        return new Timing2((int)(value * n), unit, waitingMultiple);
    }

    /**
     * Return a new timing with the standard multiple for waiting on latches, etc.
     *
     * @return this timing multiplied
     */
    @SuppressWarnings("PointlessArithmeticExpression")
    public Timing2 forWaiting()
    {
        return multiple(waitingMultiple);
    }

    /**
     * Return a new timing with a multiple that ensures a ZK session timeout
     *
     * @return this timing multiplied
     */
    public Timing2 forSessionSleep()
    {
        return multiple(SESSION_SLEEP_MULTIPLE, 1);
    }

    /**
     * Return a new timing with a multiple for sleeping a smaller amount of time
     *
     * @return this timing multiplied
     */
    public Timing2 forSleepingABit()
    {
        return multiple(.25);
    }

    /**
     * Sleep for a small amount of time
     *
     * @throws InterruptedException if interrupted
     */
    public void sleepABit() throws InterruptedException
    {
        forSleepingABit().sleep();
    }

    /**
     * Sleep for a the full amount of time
     *
     * @throws InterruptedException if interrupted
     */
    public void sleep() throws InterruptedException
    {
        unit.sleep(value);
    }

    /**
     * Return the value to use for ZK session timeout
     *
     * @return session timeout
     */
    public int session()
    {
        return multiple(SESSION_MULTIPLE).milliseconds();
    }

    /**
     * Return the value to use for ZK connection timeout
     *
     * @return connection timeout
     */
    public int connection()
    {
        return milliseconds();
    }

    private static Integer getWaitingMultiple()
    {
        return Integer.getInteger("timing-waiting-multiple", DEFAULT_WAITING_MULTIPLE);
    }
}
