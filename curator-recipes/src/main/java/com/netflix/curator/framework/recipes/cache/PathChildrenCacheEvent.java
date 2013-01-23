/*
 * Copyright 2012 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package com.netflix.curator.framework.recipes.cache;

import com.netflix.curator.framework.state.ConnectionState;

/**
 * POJO that abstracts a change to a path
 */
public class PathChildrenCacheEvent
{
    private final Type type;
    private final ChildData data;

    /**
     * Type of change
     */
    public enum Type
    {
        /**
         * A child was added to the path
         */
        CHILD_ADDED,

        /**
         * A child's data was changed
         */
        CHILD_UPDATED,

        /**
         * A child was removed from the path
         */
        CHILD_REMOVED,

        /**
         * Called when the connection has changed to {@link ConnectionState#SUSPENDED}
         */
        CONNECTION_SUSPENDED,

        /**
         * Called when the connection has changed to {@link ConnectionState#RECONNECTED}
         */
        CONNECTION_RECONNECTED,

        /**
         * Called when the connection has changed to {@link ConnectionState#LOST}
         */
        CONNECTION_LOST,

        /**
         * Posted when {@link PathChildrenCache#start(PathChildrenCache.StartMode)} is called
         * with {@link PathChildrenCache.StartMode#POST_INITIALIZED_EVENT}. This
         * event signals that the initial cache has been populated.
         */
        INITIALIZED
    }

    /**
     * @param type event type
     * @param data event data or null
     */
    public PathChildrenCacheEvent(Type type, ChildData data)
    {
        this.type = type;
        this.data = data;
    }

    /**
     * @return change type
     */
    public Type getType()
    {
        return type;
    }

    /**
     * @return the node's data
     */
    public ChildData getData()
    {
        return data;
    }

    @Override
    public String toString()
    {
        return "PathChildrenCacheEvent{" +
            "type=" + type +
            ", data=" + data +
            '}';
    }
}
