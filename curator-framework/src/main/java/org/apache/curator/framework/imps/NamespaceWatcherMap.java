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
package org.apache.curator.framework.imps;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.MapMaker;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.Watcher;
import java.io.Closeable;
import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentMap;

class NamespaceWatcherMap implements Closeable
{
    private final ConcurrentMap<Object, NamespaceWatcher> map = new MapMaker()
        .weakValues()
        .makeMap();
    private final CuratorFrameworkImpl client;

    NamespaceWatcherMap(CuratorFrameworkImpl client)
    {
        this.client = client;
    }

    @Override
    public void close()
    {
        map.clear();
    }

    @VisibleForTesting
    void drain() throws Exception
    {
        Runtime.getRuntime().gc();

        // relies on internals of MapMakerInternalMap (obviously)
        Class mapMakerInternalMapClass = Class.forName("com.google.common.collect.MapMakerInternalMap");
        Field drainThresholdField = mapMakerInternalMapClass.getDeclaredField("DRAIN_THRESHOLD");
        drainThresholdField.setAccessible(true);
        int drainThreshold = drainThresholdField.getInt(null) + 1;
        while ( drainThreshold-- > 0 )
        {
            map.get(new Object());
        }
    }

    @VisibleForTesting
    NamespaceWatcher get(Object key)
    {
        return map.get(key);
    }

    @VisibleForTesting
    boolean isEmpty()
    {
        return map.isEmpty();
    }

    NamespaceWatcher    getNamespaceWatcher(Watcher watcher)
    {
        return get(watcher, new NamespaceWatcher(client, watcher));
    }

    NamespaceWatcher    getNamespaceWatcher(CuratorWatcher watcher)
    {
        return get(watcher, new NamespaceWatcher(client, watcher));
    }

    private NamespaceWatcher    get(Object watcher, NamespaceWatcher newNamespaceWatcher)
    {
        NamespaceWatcher        existingNamespaceWatcher = map.putIfAbsent(watcher, newNamespaceWatcher);
        return (existingNamespaceWatcher != null) ? existingNamespaceWatcher : newNamespaceWatcher;
    }
}
