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
import org.apache.curator.test.compatibility.CuratorTestBase;
import org.junit.jupiter.api.Tag;
import java.util.concurrent.BlockingQueue;

@Tag(CuratorTestBase.zk36Group)
public class TestCuratorCacheEventOrdering extends TestEventOrdering<CuratorCache>
{
    @Override
    protected int getActualQty(CuratorCache cache)
    {
        return cache.size();
    }

    @Override
    protected CuratorCache newCache(CuratorFramework client, String path, BlockingQueue<Event> events)
    {
        CuratorCache cache = CuratorCache.build(client, path);
        cache.listenable().addListener((type, oldNode, node) -> {
            if ( type == CuratorCacheListener.Type.NODE_CREATED )
            {
                events.add(new Event(EventType.ADDED, node.getPath()));
            }
            else if ( type == CuratorCacheListener.Type.NODE_DELETED )
            {
                events.add(new Event(EventType.DELETED, oldNode.getPath()));
            }
        });
        cache.start();
        return cache;
    }
}
