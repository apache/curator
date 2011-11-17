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

import com.netflix.curator.framework.listen.ListenerContainer;

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
    public byte[]   getValue();

    /**
     * Returns the listenable
     *
     * @return listenable
     */
    public ListenerContainer<SharedValueListener> getListenable();
}
