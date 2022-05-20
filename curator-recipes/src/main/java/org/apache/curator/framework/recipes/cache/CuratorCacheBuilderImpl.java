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

import org.apache.curator.framework.CuratorFramework;
import java.util.function.Consumer;

class CuratorCacheBuilderImpl implements CuratorCacheBuilder
{
    private final CuratorFramework client;
    private final String path;
    private CuratorCacheStorage storage;
    private Consumer<Exception> exceptionHandler;
    private CuratorCache.Options[] options;

    CuratorCacheBuilderImpl(CuratorFramework client, String path)
    {
        this.client = client;
        this.path = path;
    }

    @Override
    public CuratorCacheBuilder withOptions(CuratorCache.Options... options)
    {
        this.options = options;
        return this;
    }

    @Override
    public CuratorCacheBuilder withStorage(CuratorCacheStorage storage)
    {
        this.storage = storage;
        return this;
    }

    @Override
    public CuratorCacheBuilder withExceptionHandler(Consumer<Exception> exceptionHandler)
    {
        this.exceptionHandler = exceptionHandler;
        return this;
    }

    @Override
    public CuratorCache build()
    {
        return new CuratorCacheImpl(client, storage, path, options, exceptionHandler);
    }
}