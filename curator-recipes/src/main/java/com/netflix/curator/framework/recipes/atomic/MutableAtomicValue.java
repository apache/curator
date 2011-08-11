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

package com.netflix.curator.framework.recipes.atomic;

class MutableAtomicValue<T> implements AtomicValue<T>
{
    T preValue;
    T postValue;
    boolean succeeded = false;
    AtomicStats stats = new AtomicStats();

    MutableAtomicValue(T preValue, T postValue)
    {
        this(preValue, postValue, false);
    }

    MutableAtomicValue(T preValue, T postValue, boolean succeeded)
    {
        this.preValue = preValue;
        this.postValue = postValue;
        this.succeeded = succeeded;
    }

    @Override
    public T preValue()
    {
        return preValue;
    }

    @Override
    public T postValue()
    {
        return postValue;
    }

    @Override
    public boolean succeeded()
    {
        return succeeded;
    }

    @Override
    public AtomicStats getStats()
    {
        return stats;
    }
}
