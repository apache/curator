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

class PathChildrenCacheListenerWrapper implements CuratorCacheListener
{
    private final PathChildrenCacheListener listener;
    private final CuratorFramework client;

    PathChildrenCacheListenerWrapper(CuratorFramework client, PathChildrenCacheListener listener)
    {
        this.listener = listener;
        this.client = client;
    }

    @Override
    public void event(Type type, ChildData oldData, ChildData data)
    {
        switch ( type )
        {
            case NODE_CREATED:
            {
                sendEvent(data, PathChildrenCacheEvent.Type.CHILD_ADDED);
                break;
            }

            case NODE_CHANGED:
            {
                sendEvent(data, PathChildrenCacheEvent.Type.CHILD_UPDATED);
                break;
            }

            case NODE_DELETED:
            {
                sendEvent(oldData, PathChildrenCacheEvent.Type.CHILD_REMOVED);
                break;
            }
        }
    }

    @Override
    public void initialized()
    {
        sendEvent(null, PathChildrenCacheEvent.Type.INITIALIZED);
    }

    private void sendEvent(ChildData node, PathChildrenCacheEvent.Type type)
    {
        PathChildrenCacheEvent event = new PathChildrenCacheEvent(type, node);
        try
        {
            listener.childEvent(client, event);
        }
        catch ( Exception e )
        {
            throw new RuntimeException(e);
        }
    }
}