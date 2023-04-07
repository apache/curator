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

package org.apache.curator.framework.recipes.cache;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.Compatibility;
import org.slf4j.LoggerFactory;
import java.util.concurrent.ExecutorService;

class CuratorCacheBridgeBuilderImpl implements CuratorCacheBridgeBuilder
{
    private final CuratorFramework client;
    private final String path;
    private CuratorCache.Options[] options;
    private boolean cacheData = true;
    private ExecutorService executorService = null;
    private final boolean forceTreeCache = Boolean.getBoolean("curator-cache-bridge-force-tree-cache");

    CuratorCacheBridgeBuilderImpl(CuratorFramework client, String path)
    {
        this.client = client;
        this.path = path;
    }

    @Override
    public CuratorCacheBridgeBuilder withOptions(CuratorCache.Options... options)
    {
        this.options = options;
        return this;
    }

    @Override
    public CuratorCacheBridgeBuilder withDataNotCached()
    {
        cacheData = false;
        return this;
    }

    @Override
    public CuratorCacheBridgeBuilder withExecutorService(ExecutorService executorService)
    {
        this.executorService = executorService;
        return this;
    }

    @Override
    public CuratorCacheBridge build()
    {
        if ( !forceTreeCache && Compatibility.hasPersistentWatchers() )
        {
            if ( executorService != null )
            {
                LoggerFactory.getLogger(getClass()).warn("CuratorCache does not support custom ExecutorService");
            }
            CuratorCacheStorage storage = cacheData ? CuratorCacheStorage.standard() : CuratorCacheStorage.dataNotCached();
            return new CuratorCacheImpl(client, storage, path, options, null);
        }
        return new CompatibleCuratorCacheBridge(client, path, options, executorService, cacheData);
    }
}