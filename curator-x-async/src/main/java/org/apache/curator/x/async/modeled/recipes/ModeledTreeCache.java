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
package org.apache.curator.x.async.modeled.recipes;

import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.x.async.modeled.ModeledDetails;
import org.apache.curator.x.async.modeled.ZPath;
import org.apache.curator.x.async.modeled.details.recipes.ModeledTreeCacheImpl;
import java.io.Closeable;
import java.util.Map;
import java.util.Optional;

public interface ModeledTreeCache<T> extends Closeable
{
    static <T> ModeledTreeCache<T> wrap(ModeledDetails<T> modeled, TreeCache cache)
    {
        return new ModeledTreeCacheImpl<>(modeled, cache);
    }

    void start();

    void close();

    Listenable<ModeledCacheListener<T>> getListenable();

    Map<ZPath, ModeledCachedNode<T>> getCurrentChildren(ZPath fullPath);

    Optional<ModeledCachedNode<T>> getCurrentData(ZPath fullPath);
}
