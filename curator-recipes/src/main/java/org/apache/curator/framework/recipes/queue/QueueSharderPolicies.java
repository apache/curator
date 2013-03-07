/*
 * Copyright 2012 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.apache.curator.framework.recipes.queue;

import com.google.common.base.Preconditions;
import java.util.concurrent.ThreadFactory;

/**
 * Various policies/options for sharding. Usage:
 * QueueSharderPolicies.builder().foo().bar().build();
 */
public class QueueSharderPolicies
{
    private int           newQueueThreshold;
    private int           thresholdCheckMs;
    private int           maxQueues;
    private ThreadFactory threadFactory;

    private static final int       DEFAULT_QUEUE_THRESHOLD = 10000;
    private static final int       DEFAULT_THRESHOLD_CHECK_MS = 30000;
    private static final int       DEFAULT_MAX_QUEUES = 10;

    public static class Builder
    {
        private QueueSharderPolicies      policies = new QueueSharderPolicies();

        /**
         * Change the queue threshold. This is the number of items that causes
         * a new queue to be added. The default is {@link QueueSharderPolicies#DEFAULT_QUEUE_THRESHOLD}
         *
         * @param newQueueThreshold new value
         * @return this
         */
        public Builder newQueueThreshold(int newQueueThreshold)
        {
            Preconditions.checkArgument(newQueueThreshold > 0, "newQueueThreshold must be a positive number");

            policies.newQueueThreshold = newQueueThreshold;
            return this;
        }

        /**
         * Change the threshold check. This is how often the queue sizes are checked. The default
         * is {@link QueueSharderPolicies#DEFAULT_THRESHOLD_CHECK_MS}
         *
         * @param thresholdCheckMs period in milliseconds
         * @return this
         */
        public Builder thresholdCheckMs(int thresholdCheckMs)
        {
            Preconditions.checkArgument(thresholdCheckMs > 0, "thresholdCheckMs must be a positive number");

            policies.thresholdCheckMs = thresholdCheckMs;
            return this;
        }

        /**
         * Change the maximum number of queues to create. The default value is {@link QueueSharderPolicies#DEFAULT_MAX_QUEUES}
         *
         * @param maxQueues the new max
         * @return this
         */
        public Builder maxQueues(int maxQueues)
        {
            Preconditions.checkArgument(maxQueues > 0, "thresholdCheckMs must be a positive number");

            policies.maxQueues = maxQueues;
            return this;
        }

        /**
         * Change the thread factory that's used to create the sharder's thread
         *
         * @param threadFactory new factory
         * @return this
         */
        public Builder threadFactory(ThreadFactory threadFactory)
        {
            policies.threadFactory = Preconditions.checkNotNull(threadFactory, "threadFactory cannot be null");
            return this;
        }

        public QueueSharderPolicies     build()
        {
            try
            {
                return policies;
            }
            finally
            {
                policies = new QueueSharderPolicies();
            }
        }

        private Builder()
        {
        }
    }

    /**
     * Allocate a new builder
     *
     * @return builder
     */
    public static Builder       builder()
    {
        return new Builder();
    }

    int getNewQueueThreshold()
    {
        return newQueueThreshold;
    }

    int getThresholdCheckMs()
    {
        return thresholdCheckMs;
    }

    int getMaxQueues()
    {
        return maxQueues;
    }

    ThreadFactory getThreadFactory()
    {
        return threadFactory;
    }

    private QueueSharderPolicies()
    {
        this.newQueueThreshold = DEFAULT_QUEUE_THRESHOLD;
        this.thresholdCheckMs = DEFAULT_THRESHOLD_CHECK_MS;
        this.maxQueues = DEFAULT_MAX_QUEUES;
        this.threadFactory = QueueBuilder.defaultThreadFactory;
    }
}
