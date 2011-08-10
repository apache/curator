package com.netflix.curator.framework.recipes.atomic;

/**
 * Debugging stats about operations
 */
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
