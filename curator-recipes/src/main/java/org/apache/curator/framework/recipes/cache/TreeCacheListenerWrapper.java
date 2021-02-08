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

class TreeCacheListenerWrapper implements CuratorCacheListener
{
    private final CuratorFramework client;
    private final TreeCacheListener listener;

    TreeCacheListenerWrapper(CuratorFramework client, TreeCacheListener listener)
    {
        this.client = client;
        this.listener = listener;
    }

    @Override
    public void event(Type type, ChildData oldData, ChildData data)
    {
        switch ( type )
        {
            case NODE_CREATED:
            {
                sendEvent(data, null, TreeCacheEvent.Type.NODE_ADDED);
                break;
            }

            case NODE_CHANGED:
            {
                sendEvent(data, oldData, TreeCacheEvent.Type.NODE_UPDATED);
                break;
            }

            case NODE_DELETED:
            {
                sendEvent(oldData, null, TreeCacheEvent.Type.NODE_REMOVED);
                break;
            }
        }
    }

    @Override
    public void initialized()
    {
        sendEvent(null, null, TreeCacheEvent.Type.INITIALIZED);
    }

    private void sendEvent(ChildData node, ChildData oldNode, TreeCacheEvent.Type type)
    {
        TreeCacheEvent event = new TreeCacheEvent(type, node, oldNode);
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