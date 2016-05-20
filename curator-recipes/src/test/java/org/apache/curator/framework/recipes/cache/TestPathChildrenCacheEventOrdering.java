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
import java.util.concurrent.BlockingQueue;

public class TestPathChildrenCacheEventOrdering extends TestEventOrdering<PathChildrenCache>
{
    @Override
    protected int getActualQty(PathChildrenCache cache)
    {
        return cache.getCurrentData().size();
    }

    @Override
    protected PathChildrenCache newCache(CuratorFramework client, String path, final BlockingQueue<Event> events) throws Exception
    {
        PathChildrenCache cache = new PathChildrenCache(client, path, false);
        PathChildrenCacheListener listener = new PathChildrenCacheListener()
        {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
            {
                if ( event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED )
                {
                    events.add(new Event(EventType.ADDED, event.getData().getPath()));
                }
                if ( event.getType() == PathChildrenCacheEvent.Type.CHILD_REMOVED )
                {
                    events.add(new Event(EventType.DELETED, event.getData().getPath()));
                }
            }
        };
        cache.getListenable().addListener(listener);
        cache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
        return cache;
    }
}
