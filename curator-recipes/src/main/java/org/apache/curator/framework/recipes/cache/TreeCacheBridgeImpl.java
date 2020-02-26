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
package org.apache.curator.framework.recipes.cache;

import com.google.common.util.concurrent.MoreExecutors;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.recipes.watch.CuratorCache;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

public class TreeCacheBridgeImpl implements TreeCacheBridge, Listenable<TreeCacheListener>
{
    private final CuratorFramework client;
    private final CuratorCache cache;
    private final Map<TreeCacheListener, ListenerBridge> listenerMap = new ConcurrentHashMap<>();

    public TreeCacheBridgeImpl(CuratorFramework client, CuratorCache cache)
    {
        this.client = Objects.requireNonNull(client, "client cannot be null");
        this.cache = Objects.requireNonNull(cache, "cache cannot be null");
    }

    @Override
    public TreeCacheBridge start()
    {
        cache.start();
        return this;
    }

    @Override
    public void close()
    {
        cache.close();
    }

    @Override
    public Listenable<TreeCacheListener> getListenable()
    {
        return this;
    }

    @Override
    public Map<String, ChildData> getCurrentChildren(String fullPath)
    {
        return ListenerBridge.toData(fullPath, cache.childrenAtPath(fullPath));
    }

    @Override
    public ChildData getCurrentData(String fullPath)
    {
        return ListenerBridge.toData(fullPath, cache.get(fullPath));
    }

    @Override
    public void addListener(TreeCacheListener listener)
    {
        addListener(listener, MoreExecutors.directExecutor());
    }

    @Override
    public void addListener(TreeCacheListener listener, Executor executor)
    {
        ListenerBridge listenerBridge = ListenerBridge.wrap(client, cache.getListenable(), listener);
        listenerBridge.add();
        listenerMap.put(listener, listenerBridge);
    }

    @Override
    public void removeListener(TreeCacheListener listener)
    {
        ListenerBridge listenerBridge = listenerMap.remove(listener);
        if ( listenerBridge != null )
        {
            listenerBridge.remove();
        }
    }
}
