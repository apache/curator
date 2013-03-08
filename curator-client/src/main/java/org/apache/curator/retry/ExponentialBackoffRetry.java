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

package org.apache.curator.retry;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Random;

/**
 * Retry policy that retries a set number of times with increasing sleep time between retries
 */
public class ExponentialBackoffRetry extends SleepingRetry
{
    private static final Logger     log = LoggerFactory.getLogger(ExponentialBackoffRetry.class);

    private static final int MAX_RETRIES_LIMIT = 29;
    private static final int DEFAULT_MAX_SLEEP_MS = Integer.MAX_VALUE;

    private final Random random = new Random();
    private final int baseSleepTimeMs;
    private final int maxSleepMs;

    /**
     * @param baseSleepTimeMs initial amount of time to wait between retries
     * @param maxRetries max number of times to retry
     */
    public ExponentialBackoffRetry(int baseSleepTimeMs, int maxRetries)
    {
        this(baseSleepTimeMs, maxRetries, DEFAULT_MAX_SLEEP_MS);
    }

    /**
     * @param baseSleepTimeMs initial amount of time to wait between retries
     * @param maxRetries max number of times to retry
     * @param maxSleepMs max time in ms to sleep on each retry
     */
    public ExponentialBackoffRetry(int baseSleepTimeMs, int maxRetries, int maxSleepMs)
    {
        super(validateMaxRetries(maxRetries));
        this.baseSleepTimeMs = baseSleepTimeMs;
        this.maxSleepMs = maxSleepMs;
    }

    @VisibleForTesting
    public int getBaseSleepTimeMs()
    {
        return baseSleepTimeMs;
    }

    @Override
    protected int getSleepTimeMs(int retryCount, long elapsedTimeMs)
    {
        // copied from Hadoop's RetryPolicies.java
        int sleepMs = baseSleepTimeMs * Math.max(1, random.nextInt(1 << (retryCount + 1)));
        if ( sleepMs > maxSleepMs )
        {
            log.warn(String.format("Sleep extension too large (%d). Pinning to %d", sleepMs, maxSleepMs));
            sleepMs = maxSleepMs;
        }
        return sleepMs;
    }

    private static int validateMaxRetries(int maxRetries)
    {
        if ( maxRetries > MAX_RETRIES_LIMIT )
        {
            log.warn(String.format("maxRetries too large (%d). Pinning to %d", maxRetries, MAX_RETRIES_LIMIT));
            maxRetries = MAX_RETRIES_LIMIT;
        }
        return maxRetries;
    }
}
