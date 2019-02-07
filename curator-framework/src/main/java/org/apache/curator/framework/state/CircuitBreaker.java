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
    private long startNanos = 0;

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
        startNanos = System.nanoTime();
        if ( tryToRetry(completion) )
        {
            return true;
        }
        close();
        return false;
    }

    boolean tryToRetry(Runnable completion)
    {
        if ( !isOpen )
        {
            return false;
        }

        long[] sleepTimeNanos = new long[]{0L};
        RetrySleeper retrySleeper = (time, unit) -> sleepTimeNanos[0] = unit.toNanos(time);
        if ( retryPolicy.allowRetry(retryCount, System.nanoTime() - startNanos, retrySleeper) )
        {
            ++retryCount;
            service.schedule(completion, sleepTimeNanos[0], TimeUnit.NANOSECONDS);
            return true;
        }
        return false;
    }

    boolean close()
    {
        boolean wasOpen = isOpen;
        retryCount = 0;
        isOpen = false;
        startNanos = 0;
        return wasOpen;
    }
}
