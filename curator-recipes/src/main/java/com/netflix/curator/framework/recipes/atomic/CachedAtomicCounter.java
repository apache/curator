package com.netflix.curator.framework.recipes.atomic;

/**
 * Uses an {@link AtomicCounter} and allocates values in chunks for better performance
 */
public class CachedAtomicCounter
{
    private final AtomicCounter<Long>  counter;
    private final long                 cacheFactor;

    private AtomicValue<Long>          currentValue = null;
    private int                        currentIndex = 0;

    /**
     * @param counter the counter to use
     * @param cacheFactor the number of values to allocate at a time
     */
    public CachedAtomicCounter(AtomicCounter<Long> counter, int cacheFactor)
    {
        this.counter = counter;
        this.cacheFactor = cacheFactor;
    }

    /**
     * Returns the next value (incrementing by 1). If a new chunk of numbers is needed, it is
     * requested from the counter
     *
     * @return next increment
     * @throws Exception errors
     */
    public AtomicValue<Long>       next() throws Exception
    {
        MutableAtomicValue<Long> result = new MutableAtomicValue<Long>(0L, 0L);

        if ( currentValue == null )
        {
            currentValue = counter.add(cacheFactor);
            if ( !currentValue.succeeded() )
            {
                currentValue = null;
                result.succeeded = false;
                return result;
            }
            currentIndex = 0;
        }

        result.succeeded = true;
        result.preValue = currentValue.preValue() + currentIndex;
        result.postValue = result.preValue + 1;

        if ( ++currentIndex >= cacheFactor )
        {
            currentValue = null;
        }

        return result;
    }
}
