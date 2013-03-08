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

package org.apache.curator.framework.recipes.atomic;

public interface DistributedAtomicNumber<T>
{
    /**
     * Returns the current value of the counter. NOTE: if the value has never been set,
     * <code>0</code> is returned.
     *
     * @return value info
     * @throws Exception ZooKeeper errors
     */
    public AtomicValue<T>     get() throws Exception;

    /**
     * Atomically sets the value to the given updated value
     * if the current value {@code ==} the expected value.
     * Remember to always check {@link AtomicValue#succeeded()}.
     *
     *
     * @param expectedValue the expected value
     * @param newValue the new value for the counter
     * @return value info
     * @throws Exception ZooKeeper errors
     */
    public AtomicValue<T>    compareAndSet(T expectedValue, T newValue) throws Exception;

    /**
     * Attempt to atomically set the value to the given value. Remember to always
     * check {@link AtomicValue#succeeded()}.
     *
     * @param newValue the value to set
     * @return value info
     * @throws Exception ZooKeeper errors
     */
    public AtomicValue<T>    trySet(T newValue) throws Exception;

    /**
     * Forcibly sets the value of the counter without any guarantees of atomicity.
     *
     * @param newValue the new value
     * @throws Exception ZooKeeper errors
     */
    public void              forceSet(T newValue) throws Exception;

    /**
     * Add 1 to the current value and return the new value information. Remember to always
     * check {@link AtomicValue#succeeded()}.
     *
     * @return value info
     * @throws Exception ZooKeeper errors
     */
    public AtomicValue<T>    increment() throws Exception;

    /**
     * Subtract 1 from the current value and return the new value information. Remember to always
     * check {@link AtomicValue#succeeded()}.
     *
     * @return value info
     * @throws Exception ZooKeeper errors
     */
    public AtomicValue<T>    decrement() throws Exception;

    /**
     * Add delta to the current value and return the new value information. Remember to always
     * check {@link AtomicValue#succeeded()}.
     *
     * @param delta amount to add
     * @return value info
     * @throws Exception ZooKeeper errors
     */
    public AtomicValue<T>    add(T delta) throws Exception;

    /**
     * Subtract delta from the current value and return the new value information. Remember to always
     * check {@link AtomicValue#succeeded()}.
     *
     * @param delta amount to subtract
     * @return value info
     * @throws Exception ZooKeeper errors
     */
    public AtomicValue<T>    subtract(T delta) throws Exception;
}
