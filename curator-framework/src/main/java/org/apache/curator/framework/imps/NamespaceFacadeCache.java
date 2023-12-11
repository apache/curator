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

package org.apache.curator.framework.imps;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

class NamespaceFacadeCache {
    private final CuratorFrameworkImpl client;
    private final AtomicReference<NamespaceFacade> nullNamespace;
    private final CacheLoader<String, NamespaceFacade> loader = new CacheLoader<String, NamespaceFacade>() {
        @Override
        public NamespaceFacade load(String namespace) throws Exception {
            return new NamespaceFacade(client, namespace);
        }
    };
    private final LoadingCache<String, NamespaceFacade> cache = CacheBuilder.newBuilder()
            .expireAfterAccess(5, TimeUnit.MINUTES) // does this need config? probably not
            .build(loader);

    NamespaceFacadeCache(CuratorFrameworkImpl client) {
        this.client = client;
        this.nullNamespace = new AtomicReference<>(null);
    }

    private NamespaceFacade getNullNamespace() {
        NamespaceFacade facade = nullNamespace.get();
        if (facade != null) {
            return facade;
        }
        facade = new NamespaceFacade(client, null);
        if (!nullNamespace.compareAndSet(null, facade)) {
            facade = nullNamespace.get();
        }
        return facade;
    }

    NamespaceFacade get(String namespace) {
        try {
            if (namespace == null) {
                return getNullNamespace();
            }
            return cache.get(namespace);
        } catch (ExecutionException e) {
            throw new RuntimeException(e); // should never happen
        }
    }
}
