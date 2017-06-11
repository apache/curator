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
package org.apache.curator.x.async;

import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.utils.ThreadUtils;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *     Utility for acquiring a lock asynchronously
 * </p>
 *
 * <p>
 *     Canonical usage:
 * <code><pre>
 *     InterProcessMutex mutex = new InterProcessMutex(...) // or any InterProcessLock
 *     AsyncLocker.lockAsync(mutex).handle((state, e) -> {
 *         if ( e != null )
 *         {
 *             // handle the error
 *         }
 *         else if ( state.hasTheLock() )
 *         {
 *             try
 *             {
 *                 // do work while holding the lock
 *             }
 *             finally
 *             {
 *                 state.release();
 *             }
 *         }
 *     });
 * </pre></code>
 * </p>
 */
public class AsyncLocker
{
    /**
     * State of the lock
     */
    public interface LockState
    {
        /**
         * Returns true if you own the lock
         *
         * @return true/false
         */
        boolean hasTheLock();

        /**
         * Safe release of the lock. Only tries to release
         * if you own the lock. The lock ownership is changed
         * to <code>false</code> by this method.
         */
        void release();
    }

    /**
     * Attempt to acquire the given lock asynchronously using the given timeout and executor.
     *
     * @param lock a lock implementation (e.g. {@link org.apache.curator.framework.recipes.locks.InterProcessMutex},
     * {@link org.apache.curator.framework.recipes.locks.InterProcessSemaphoreV2}, etc.)
     * @param timeout max timeout to acquire lock
     * @param unit time unit of timeout
     * @param executor executor to use to asynchronously acquire
     * @return stage
     */
    public static CompletionStage<LockState> lockAsync(InterProcessLock lock, long timeout, TimeUnit unit, Executor executor)
    {
        if ( executor == null )
        {
            return CompletableFuture.supplyAsync(() -> lock(lock, timeout, unit));
        }
        return CompletableFuture.supplyAsync(() -> lock(lock, timeout, unit), executor);
    }

    /**
     * Attempt to acquire the given lock asynchronously using the given executor and without a timeout.
     *
     * @param lock a lock implementation (e.g. {@link org.apache.curator.framework.recipes.locks.InterProcessMutex},
     * {@link org.apache.curator.framework.recipes.locks.InterProcessSemaphoreV2}, etc.)
     * @param executor executor to use to asynchronously acquire
     * @return stage
     */
    public static CompletionStage<LockState> lockAsync(InterProcessLock lock, Executor executor)
    {
        return lockAsync(lock, 0, null, executor);
    }

    /**
     * Attempt to acquire the given lock asynchronously using the given timeout using the {@link java.util.concurrent.ForkJoinPool#commonPool()}.
     *
     * @param lock a lock implementation (e.g. {@link org.apache.curator.framework.recipes.locks.InterProcessMutex},
     * {@link org.apache.curator.framework.recipes.locks.InterProcessSemaphoreV2}, etc.)
     * @param timeout max timeout to acquire lock
     * @param unit time unit of timeout
     * @return stage
     */
    public static CompletionStage<LockState> lockAsync(InterProcessLock lock, long timeout, TimeUnit unit)
    {
        return lockAsync(lock, timeout, unit, null);
    }

    /**
     * Attempt to acquire the given lock asynchronously without timeout using the {@link java.util.concurrent.ForkJoinPool#commonPool()}.
     *
     * @param lock a lock implementation (e.g. {@link org.apache.curator.framework.recipes.locks.InterProcessMutex},
     * {@link org.apache.curator.framework.recipes.locks.InterProcessSemaphoreV2}, etc.)
     * @return stage
     */
    public static CompletionStage<LockState> lockAsync(InterProcessLock lock)
    {
        return lockAsync(lock, 0, null, null);
    }

    private static LockState lock(InterProcessLock lock, long timeout, TimeUnit unit)
    {
        try
        {
            if ( unit != null )
            {
                boolean hasTheLock = lock.acquire(timeout, unit);
                return new InternalLockState(lock, hasTheLock);
            }

            lock.acquire();
            return new InternalLockState(lock, true);
        }
        catch ( Exception e )
        {
            ThreadUtils.checkInterrupted(e);
            throw new RuntimeException(e);
        }
    }

    private AsyncLocker()
    {
    }

    private static class InternalLockState implements LockState
    {
        private final InterProcessLock lock;
        private volatile boolean hasTheLock;

        public InternalLockState(InterProcessLock lock, boolean hasTheLock)
        {
            this.lock = lock;
            this.hasTheLock = hasTheLock;
        }

        @Override
        public boolean hasTheLock()
        {
            return hasTheLock;
        }

        @Override
        public void release()
        {
            if ( hasTheLock )
            {
                hasTheLock = false;
                try
                {
                    lock.release();
                }
                catch ( Exception e )
                {
                    ThreadUtils.checkInterrupted(e);
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
