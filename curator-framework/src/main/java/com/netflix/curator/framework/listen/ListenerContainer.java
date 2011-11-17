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

package com.netflix.curator.framework.listen;

import com.google.common.base.Function;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * Abstracts an object that has listeners
 */
public class ListenerContainer<T> implements Listenable<T>
{
    private final Map<T, ListenerEntry<T>>      listeners = Maps.newConcurrentMap();

    @Override
    public void addListener(T listener)
    {
        addListener(listener, MoreExecutors.sameThreadExecutor());
    }

    @Override
    public void addListener(T listener, Executor executor)
    {
        listeners.put(listener, new ListenerEntry<T>(listener, executor));
    }

    @Override
    public void removeListener(T listener)
    {
        listeners.remove(listener);
    }

    /**
     * Remove all listeners
     */
    public void     clear()
    {
        listeners.clear();
    }

    /**
     * Return the number of listeners
     *
     * @return number
     */
    public int      size()
    {
        return listeners.size();
    }

    /**
     * Utility - apply the given function to each listener. The function receives
     * the listener as an argument.
     *
     * @param function function to call for each listener
     */
    public void     forEach(Function<T, Void> function)
    {
        for ( final ListenerEntry<T> entry : listeners.values() )
        {
            function.apply(entry.listener);
        }
    }
}
