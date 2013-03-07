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

package org.apache.curator.framework.recipes.atomic;

/**
 * Uses an {@link DistributedAtomicNumber} and allocates values in chunks for better performance
 */
public class CachedAtomicInteger
{
    private final DistributedAtomicInteger  number;
    private final int                       cacheFactor;

    private AtomicValue<Integer>       currentValue = null;
    private int                        currentIndex = 0;

    /**
     * @param number the number to use
     * @param cacheFactor the number of values to allocate at a time
     */
    public CachedAtomicInteger(DistributedAtomicInteger number, int cacheFactor)
    {
        this.number = number;
        this.cacheFactor = cacheFactor;
    }

    /**
     * Returns the next value (incrementing by 1). If a new chunk of numbers is needed, it is
     * requested from the number
     *
     * @return next increment
     * @throws Exception errors
     */
    public AtomicValue<Integer>       next() throws Exception
    {
        MutableAtomicValue<Integer> result = new MutableAtomicValue<Integer>(0, 0);

        if ( currentValue == null )
        {
            currentValue = number.add(cacheFactor);
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
