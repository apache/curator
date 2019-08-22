/**
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

import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * Non mapping version of a listener container
 */
public class StandardListenerManager<T> implements UnaryListenerManager<T>
{
    private final ListenerManager<T, T> container;

    /**
     * Returns a new standard listener container
     *
     * @return new container
     */
    public static <T> StandardListenerManager<T> standard()
    {
        MappingListenerManager<T, T> container = new MappingListenerManager<>(Function.identity());
        return new StandardListenerManager<>(container);
    }

    /**
     * Returns a new mapping container that maps to the same type
     *
     * @param mapper listener mapper/wrapper
     * @return new container
     */
    public static <T> StandardListenerManager<T> mappingStandard(UnaryOperator<T> mapper)
    {
        MappingListenerManager<T, T> container = new MappingListenerManager<>(mapper);
        return new StandardListenerManager<>(container);
    }

    @Override
    public void addListener(T listener)
    {
        container.addListener(listener);
    }

    @Override
    public void addListener(T listener, Executor executor)
    {
        container.addListener(listener, executor);
    }

    @Override
    public void removeListener(T listener)
    {
        container.removeListener(listener);
    }

    @Override
    public void clear()
    {
        container.clear();
    }

    @Override
    public int size()
    {
        return container.size();
    }

    @Override
    public void forEach(Consumer<T> function)
    {
        container.forEach(function);
    }

    private StandardListenerManager(ListenerManager<T, T> container)
    {
        this.container = container;
    }
}
