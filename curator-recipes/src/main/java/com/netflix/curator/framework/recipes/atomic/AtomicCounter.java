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

public interface AtomicCounter<T>
{
    /**
     * Returns the current value of the counter. NOTE: if the value has never been set,
     * <code>0</code> is returned.
     *
     * @return the current value
     * @throws Exception ZooKeeper errors
     */
    public AtomicValue<T>     get() throws Exception;

    /**
     * Add 1 to the current value and return the new value information. Remember to always
     * check {@link AtomicValue#succeeded()}.
     *
     * @return the current value
     * @throws Exception ZooKeeper errors
     */
    public AtomicValue<T>    increment() throws Exception;

    /**
     * Subtract 1 from the current value and return the new value information. Remember to always
     * check {@link AtomicValue#succeeded()}.
     *
     * @return the current value
     * @throws Exception ZooKeeper errors
     */
    public AtomicValue<T>    decrement() throws Exception;

    /**
     * Add delta to the current value and return the new value information. Remember to always
     * check {@link AtomicValue#succeeded()}.
     *
     * @param delta amount to add
     * @return the current value
     * @throws Exception ZooKeeper errors
     */
    public AtomicValue<T>    add(T delta) throws Exception;

    /**
     * Subtract delta from the current value and return the new value information. Remember to always
     * check {@link AtomicValue#succeeded()}.
     *
     * @param delta amount to subtract
     * @return the current value
     * @throws Exception ZooKeeper errors
     */
    public AtomicValue<T>    subtract(T delta) throws Exception;
}
