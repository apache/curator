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

package org.apache.curator.framework.imps;

import com.google.common.annotations.VisibleForTesting;
import org.apache.curator.RetrySleeper;
import org.apache.curator.framework.api.BackgroundCallback;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

class OperationAndData<T> implements Delayed, RetrySleeper
{
    private static final AtomicLong nextOrdinal = new AtomicLong();

    private final BackgroundOperation<T> operation;
    private final T data;
    private final BackgroundCallback callback;
    private final long startTimeMs = System.currentTimeMillis();
    private final ErrorCallback<T> errorCallback;
    private final AtomicInteger retryCount = new AtomicInteger(0);
    private final AtomicLong sleepUntilTimeMs = new AtomicLong(0);
    private final long ordinal = nextOrdinal.getAndIncrement();
    private final Object context;

    interface ErrorCallback<T>
    {
        void retriesExhausted(OperationAndData<T> operationAndData);
    }

    OperationAndData(BackgroundOperation<T> operation, T data, BackgroundCallback callback, ErrorCallback<T> errorCallback, Object context)
    {
        this.operation = operation;
        this.data = data;
        this.callback = callback;
        this.errorCallback = errorCallback;
        this.context = context;
    }

    Object getContext()
    {
        return context;
    }

    void callPerformBackgroundOperation() throws Exception
    {
        operation.performBackgroundOperation(this);
    }

    T getData()
    {
        return data;
    }

    long getElapsedTimeMs()
    {
        return System.currentTimeMillis() - startTimeMs;
    }

    int getThenIncrementRetryCount()
    {
        return retryCount.getAndIncrement();
    }

    BackgroundCallback getCallback()
    {
        return callback;
    }

    ErrorCallback<T> getErrorCallback()
    {
        return errorCallback;
    }

    @VisibleForTesting
    BackgroundOperation<T> getOperation()
    {
        return operation;
    }

    @Override
    public void sleepFor(long time, TimeUnit unit) throws InterruptedException
    {
        sleepUntilTimeMs.set(System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert(time, unit));
    }

    @Override
    public long getDelay(TimeUnit unit)
    {
        return unit.convert(sleepUntilTimeMs.get() - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed o)
    {
        if ( o == this )
        {
            return 0;
        }

        long diff = getDelay(TimeUnit.MILLISECONDS) - o.getDelay(TimeUnit.MILLISECONDS);
        if ( diff == 0 )
        {
            if ( o instanceof OperationAndData )
            {
                diff = ordinal - ((OperationAndData)o).ordinal;
            }
        }

        return (diff < 0) ? -1 : ((diff > 0) ? 1 : 0);
    }
}
