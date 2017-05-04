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
package org.apache.curator.x.async.modeled.cached;

import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.x.async.modeled.ModeledFramework;
import org.apache.curator.x.async.modeled.ZPath;
import java.io.Closeable;

public interface CachedModeledFramework<T> extends ModeledFramework<T>, Closeable
{
    /**
     * Return the cache instance
     *
     * @return cache
     */
    ModeledCache<T> getCache();

    /**
     * Start the internally created cache
     */
    void start();

    /**
     * Close/stop the internally created cache
     */
    @Override
    void close();

    /**
     * Return the listener container so that you can add/remove listeners
     *
     * @return listener container
     */
    Listenable<ModeledCacheListener<T>> listenable();

    /**
     * {@inheritDoc}
     */
    @Override
    CachedModeledFramework<T> at(Object child);

    /**
     * {@inheritDoc}
     */
    @Override
    CachedModeledFramework<T> withPath(ZPath path);
}
