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

/**
 * Abstracts a value returned from one of the Atomics
 */
public interface AtomicValue<T>
{
    /**
     * <b>MUST be checked.</b> Returns true if the operation succeeded. If false is returned,
     * the operation failed and the atomic was not updated.
     *
     * @return true/false
     */
    public boolean      succeeded();

    /**
     * Returns the value of the counter prior to the operation
     *
     * @return pre-operation value
     */
    public T            preValue();

    /**
     * Returns the value of the counter after to the operation
     *
     * @return post-operation value
     */
    public T            postValue();

    /**
     * Returns debugging stats about the operation
     *
     * @return stats
     */
    public AtomicStats  getStats();
}
