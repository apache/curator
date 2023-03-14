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
    private final AtomicBoolean acquired = new AtomicBoolean(false);

    /**
     * @param lock a lock implementation (e.g. {@link InterProcessMutex}, {@link InterProcessSemaphoreV2}, etc.)
     * @param timeout max timeout to acquire lock
     * @param unit time unit of timeout
     * @throws Exception Curator errors or {@link TimeoutException} if the lock cannot be acquired within the timeout
     */
    public Locker(InterProcessLock lock, long timeout, TimeUnit unit) throws Exception
    {
        this.lock = lock;
        acquired.set(acquireLock(lock, timeout, unit));
        if ( !acquired.get() )
        {
            throw new TimeoutException("Could not acquire lock within timeout of " + unit.toMillis(timeout) + "ms");
        }
    }

    /**
     * @param lock a lock implementation (e.g. {@link InterProcessMutex}, {@link InterProcessSemaphoreV2}, etc.)
     * @throws Exception errors
     */
    public Locker(InterProcessLock lock) throws Exception
    {
        this.lock = lock;
        acquireLock(lock);
        acquired.set(true);
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

    protected void acquireLock(InterProcessLock lock) throws Exception
    {
        lock.acquire();
    }

    protected boolean acquireLock(InterProcessLock lock, long timeout, TimeUnit unit) throws Exception
    {
        return lock.acquire(timeout, unit);
    }
}
