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

package com.netflix.curator.framework.recipes.shared;

import java.util.concurrent.Executor;

/**
 * Abstracts a shared value and allows listening for changes to the value
 */
public interface SharedValueReader
{
    /**
     * Return the current value of the count
     *
     * @return count
     */
    byte[]   getValue();

    /**
     * Add a listener for changes to the count
     *
     * @param listener the listener
     */
    void     addListener(SharedValueListener listener);

    /**
     * Add a listener for changes to the count
     *
     * @param listener the listener
     * @param executor executor to use when notifying the listener
     */
    void     addListener(SharedValueListener listener, Executor executor);

    /**
     * Remove the listener
     *
     * @param listener the listener
     */
    void     removeListener(SharedValueListener listener);
}
