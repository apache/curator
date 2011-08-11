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
package com.netflix.curator.retry;

import java.util.Random;

/**
 * Retry policy that retries a set number of times with increasing sleep time between retries
 */
public class ExponentialBackoffRetry extends SleepingRetry
{
    private final Random random = new Random();
    private final int baseSleepTimeMs;

    /**
     * @param baseSleepTimeMs initial amount of time to wait between retries
     * @param maxRetries max number of times to retry
     */
    public ExponentialBackoffRetry(int baseSleepTimeMs, int maxRetries)
    {
        super(maxRetries);
        this.baseSleepTimeMs = baseSleepTimeMs;
    }

    // made public for testing
    public int getBaseSleepTimeMs()
    {
        return baseSleepTimeMs;
    }

    @Override
    protected int getSleepTimeMs(int retryCount, long elapsedTimeMs)
    {
        // copied from Hadoop's RetryPolicies.java
        return baseSleepTimeMs * Math.max(1, random.nextInt(1 << (retryCount+1)));
    }
}
