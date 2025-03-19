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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.curator.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Version of ListenerManager that supports mapping/wrapping of listeners
 */
public class MappingListenerManager<K, V> implements ListenerManager<K, V> {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Map<K, ListenerEntry<V>> listeners = new ConcurrentHashMap<>();
    private final Function<K, V> mapper;

    /**
     * Returns a new container that wraps listeners using the given mapper
     *
     * @param mapper listener mapper/wrapper
     * @return new container
     */
    public static <K, V> ListenerManager<K, V> mapping(Function<K, V> mapper) {
        return new MappingListenerManager<>(mapper);
    }

    @Override
    public void addListener(K listener) {
        addListener(listener, Runnable::run);
    }

    @Override
    public void addListener(K listener, Executor executor) {
        V mapped = mapper.apply(listener);
        listeners.put(listener, new ListenerEntry<>(mapped, executor));
    }

    @Override
    public void removeListener(K listener) {
        if (listener != null) {
            listeners.remove(listener);
        }
    }

    @Override
    public void clear() {
        listeners.clear();
    }

    @Override
    public int size() {
        return listeners.size();
    }

    @Override
    public void forEach(Consumer<V> function) {
        for (ListenerEntry<V> entry : listeners.values()) {
            entry.executor.execute(() -> {
                try {
                    function.accept(entry.listener);
                } catch (Throwable e) {
                    ThreadUtils.checkInterrupted(e);
                    log.error("Listener ({}) threw an exception", entry.listener, e);
                }
            });
        }
    }

    MappingListenerManager(Function<K, V> mapper) {
        this.mapper = mapper;
    }
}
