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

package com.netflix.curator.framework.recipes.atomic;

public class AtomicStats
{
    private int     optimisticTries = 0;
    private int     promotedLockTries = 0;
    private long    optimisticTimeMs = 0;
    private long    promotedTimeMs = 0;

    /**
     * Returns the number of optimistic locks used to perform the operation
     *
     * @return qty
     */
    public int getOptimisticTries()
    {
        return optimisticTries;
    }

    /**
     * Returns the number of mutex locks used to perform the operation
     *
     * @return qty
     */
    public int getPromotedLockTries()
    {
        return promotedLockTries;
    }

    /**
     * Returns the time spent trying the operation with optimistic locks
     *
     * @return time in ms
     */
    public long getOptimisticTimeMs()
    {
        return optimisticTimeMs;
    }

    /**
     * Returns the time spent trying the operation with mutex locks
     *
     * @return time in ms
     */
    public long getPromotedTimeMs()
    {
        return promotedTimeMs;
    }

    void incrementOptimisticTries()
    {
        ++optimisticTries;
    }

    void incrementPromotedTries()
    {
        ++promotedLockTries;
    }

    void setOptimisticTimeMs(long optimisticTimeMs)
    {
        this.optimisticTimeMs = optimisticTimeMs;
    }

    void setPromotedTimeMs(long promotedTimeMs)
    {
        this.promotedTimeMs = promotedTimeMs;
    }
}
