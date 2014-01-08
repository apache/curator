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

package org.apache.curator.x.rest.system;

import com.google.common.collect.Lists;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import java.util.List;

public class PathChildrenCacheThing
{
    private final PathChildrenCache cache;
    private final List<PathChildrenCacheEvent> events = Lists.newLinkedList();

    public PathChildrenCacheThing(PathChildrenCache cache)
    {
        this.cache = cache;
    }

    public PathChildrenCache getCache()
    {
        return cache;
    }

    public synchronized List<PathChildrenCacheEvent> blockForPendingEvents() throws InterruptedException
    {
        while ( events.size() == 0 )
        {
            wait();
        }

        List<PathChildrenCacheEvent> localEvents = Lists.newArrayList(events);
        events.clear();
        return localEvents;
    }

    public synchronized void addEvent(PathChildrenCacheEvent event)
    {
        events.add(event);
        notifyAll();
    }
}
