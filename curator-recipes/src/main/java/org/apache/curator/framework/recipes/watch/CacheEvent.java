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
 * Cache change event types
 */
public enum CacheEvent
{
    /**
     * A node was added to the cache that wasn't previously in the cache
     */
    NODE_CREATED,

    /**
     * A node was removed from the cache
     */
    NODE_DELETED,

    /**
     * A node in the cache was changed due to the {@link CachedNodeComparator} signifying
     * a difference
     */
    NODE_CHANGED,

    /**
     * The cache was refreshed to a call to one of the refresh() methods, server reconnection,
     * etc. NOTE: this event is only sent if {@link CuratorCacheBuilder#sendingRefreshEvents(boolean)}
     * is true (default is <code>true</code>).
     */
    CACHE_REFRESHED
}
