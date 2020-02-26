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
package org.apache.curator.framework.recipes.watch;

/**
 * Functor for listening for events on a cache
 */
public interface CacheListener
{
    /**
     * <p>
     *     A event was posted for the cache. The meaning of the other arguments depends on the event
     *     type
     * </p>
     *
     * <p>
     *     <code>path</code> is generally the affected path. For {@link CacheEvent#CACHE_REFRESHED}
     *     it will be the path will be the path passed to the refresh() method used or the main path
     *     used to construct the cache.
     * </p>
     *
     * <p>
     *     For {@link CacheEvent#NODE_CREATED} and {@link CacheEvent#NODE_CHANGED}
     *     <code>affectedNode</code> is the new value of the node. For {@link CacheEvent#NODE_DELETED}
     *     <code>affectedNode</code> is the value of the node being deleted.
     * </p>
     *
     * @param event the event
     * @param path the path of the event
     * @param affectedNode the affected node value
     */
    void process(CacheEvent event, String path, CachedNode affectedNode);
}
