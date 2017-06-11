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
 *     AsyncLocker.lockAsync(mutex).thenAccept(dummy -> {
 *         try
 *         {
 *             // do work while holding the lock
 *         }
 *         finally
 *         {
 *             AsyncLocker.release(mutex);
 *         }
 *     }).exceptionally(e -> {
 *         if ( e instanceOf TimeoutException ) {
 *             // timed out trying to acquire the lock
 *         }
 *         // handle the error
 *         return null;
 *     });
 * </pre></code>
 * </p>
 */
public class AsyncLocker
{
    /**
     * Set as the completion stage's exception when trying to acquire a lock
     * times out
     */
    public static class TimeoutException extends RuntimeException
    {
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
    public static CompletionStage<Void> lockAsync(InterProcessLock lock, long timeout, TimeUnit unit, Executor executor)
    {
        CompletableFuture<Void> future = new CompletableFuture<>();
        if ( executor == null )
        {
            CompletableFuture.runAsync(() -> lock(future, lock, timeout, unit));
        }
        else
        {
            CompletableFuture.runAsync(() -> lock(future, lock, timeout, unit), executor);
        }
        return future;
    }

    /**
     * Attempt to acquire the given lock asynchronously using the given executor and without a timeout.
     *
     * @param lock a lock implementation (e.g. {@link org.apache.curator.framework.recipes.locks.InterProcessMutex},
     * {@link org.apache.curator.framework.recipes.locks.InterProcessSemaphoreV2}, etc.)
     * @param executor executor to use to asynchronously acquire
     * @return stage
     */
    public static CompletionStage<Void> lockAsync(InterProcessLock lock, Executor executor)
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
    public static CompletionStage<Void> lockAsync(InterProcessLock lock, long timeout, TimeUnit unit)
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
    public static CompletionStage<Void> lockAsync(InterProcessLock lock)
    {
        return lockAsync(lock, 0, null, null);
    }

    /**
     * Release the lock and wrap any exception in <code>RuntimeException</code>
     *
     * @param lock lock to release
     */
    public static void release(InterProcessLock lock)
    {
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

    private static void lock(CompletableFuture<Void> future, InterProcessLock lock, long timeout, TimeUnit unit)
    {
        try
        {
            if ( unit != null )
            {
                if ( lock.acquire(timeout, unit) )
                {
                    future.complete(null);
                }
                else
                {
                    future.completeExceptionally(new TimeoutException());
                }
            }
            else
            {
                lock.acquire();
                future.complete(null);
            }
        }
        catch ( Exception e )
        {
            ThreadUtils.checkInterrupted(e);
            future.completeExceptionally(e);
        }
    }

    private AsyncLocker()
    {
    }
}
