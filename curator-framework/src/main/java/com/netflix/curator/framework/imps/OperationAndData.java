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
package com.netflix.curator.framework.imps;

import com.netflix.curator.framework.api.BackgroundCallback;

class OperationAndData<T>
{
    private final BackgroundOperation<T>    operation;
    private final T                         data;
    private final BackgroundCallback        callback;
    private final long                      startTimeMs = System.currentTimeMillis();
    private final ErrorCallback<T>          errorCallback;

    private int     retryCount = 0;

    interface ErrorCallback<T>
    {
        void retriesExhausted(OperationAndData<T> operationAndData);
    }

    OperationAndData(BackgroundOperation<T> operation, T data, BackgroundCallback callback, ErrorCallback<T> errorCallback)
    {
        this.operation = operation;
        this.data = data;
        this.callback = callback;
        this.errorCallback = errorCallback;
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
        return retryCount++;
    }

    BackgroundCallback getCallback()
    {
        return callback;
    }

    ErrorCallback<T> getErrorCallback()
    {
        return errorCallback;
    }
}
