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

package org.apache.curator.framework.listen;

import java.util.function.Consumer;

public interface ListenerManager<K, V> extends Listenable<K>
{
    /**
     * Remove all listeners
     */
    void clear();

    /**
     * Return the number of listeners
     *
     * @return number
     */
    int size();

    /**
     * Utility - apply the given function to each listener. The function receives
     * the listener as an argument.
     *
     * @param function function to call for each listener
     */
    void forEach(Consumer<V> function);

    default boolean isEmpty()
    {
        return size() == 0;
    }
}
