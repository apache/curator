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
import java.util.concurrent.BlockingQueue;

public class TestTreeCacheEventOrdering extends TestEventOrdering<TreeCache>
{
    @Override
    protected int getActualQty(TreeCache cache)
    {
        return cache.getCurrentChildren("/root").size();
    }

    @Override
    protected TreeCache newCache(CuratorFramework client, String path, final BlockingQueue<Event> events) throws Exception
    {
        TreeCache cache = new TreeCache(client, path);
        TreeCacheListener listener = new TreeCacheListener()
        {
            @Override
            public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception
            {
                if ( (event.getData() != null) && (event.getData().getPath().startsWith("/root/")) )
                {
                    if ( event.getType() == TreeCacheEvent.Type.NODE_ADDED )
                    {
                        events.add(new Event(EventType.ADDED, event.getData().getPath()));
                    }
                    if ( event.getType() == TreeCacheEvent.Type.NODE_REMOVED )
                    {
                        events.add(new Event(EventType.DELETED, event.getData().getPath()));
                    }
                }
            }
        };
        cache.getListenable().addListener(listener);
        cache.start();
        return cache;
    }
}
