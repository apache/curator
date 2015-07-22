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

/**
 * POJO that abstracts a change to a path
 */
public class TreeCacheEvent
{
    private final Type type;
    private final ChildData data;

    /**
     * Type of change
     */
    public enum Type
    {
        /**
         * A node was added.
         */
        NODE_ADDED,

        /**
         * A node's data was changed
         */
        NODE_UPDATED,

        /**
         * A node was removed from the tree
         */
        NODE_REMOVED,

        /**
         * Called when the connection has changed to {@link org.apache.curator.framework.state.ConnectionState#SUSPENDED}
         * <p>
         * This is exposed so that users of the class can be notified of issues that *might* affect normal operation.
         * The TreeCache is written such that listeners are not expected to do anything special on this
         * event, except for those people who want to cause some application-specific logic to fire when this occurs.
         * While the connection is down, the TreeCache will continue to have its state from before it lost
         * the connection and after the connection is restored, the TreeCache will emit normal child events
         * for all of the adds, deletes and updates that happened during the time that it was disconnected.
         * </p>
         */
        CONNECTION_SUSPENDED,

        /**
         * Called when the connection has changed to {@link org.apache.curator.framework.state.ConnectionState#RECONNECTED}
         * <p>
         * This is exposed so that users of the class can be notified of issues that *might* affect normal operation.
         * The TreeCache is written such that listeners are not expected to do anything special on this
         * event, except for those people who want to cause some application-specific logic to fire when this occurs.
         * While the connection is down, the TreeCache will continue to have its state from before it lost
         * the connection and after the connection is restored, the TreeCache will emit normal child events
         * for all of the adds, deletes and updates that happened during the time that it was disconnected.
         * </p><p>
         * After reconnection, the cache will resynchronize its internal state with the server, then fire a
         * {@link #INITIALIZED} event.
         * </p>
         */
        CONNECTION_RECONNECTED,

        /**
         * Called when the connection has changed to {@link org.apache.curator.framework.state.ConnectionState#LOST}
         * <p>
         * This is exposed so that users of the class can be notified of issues that *might* affect normal operation.
         * The TreeCache is written such that listeners are not expected to do anything special on this
         * event, except for those people who want to cause some application-specific logic to fire when this occurs.
         * While the connection is down, the TreeCache will continue to have its state from before it lost
         * the connection and after the connection is restored, the TreeCache will emit normal child events
         * for all of the adds, deletes and updates that happened during the time that it was disconnected.
         * </p>
         */
        CONNECTION_LOST,

        /**
         * Posted after the initial cache has been fully populated.
         * <p>
         * On startup, the cache synchronizes its internal
         * state with the server, publishing a series of {@link #NODE_ADDED} events as new nodes are discovered.  Once
         * the cachehas been fully synchronized, this {@link #INITIALIZED} this event is published.  All events
         * published after this event represent actual server-side mutations.
         * </p><p>
         * On reconnection, the cache will resynchronize its internal state with the server, and fire this event again
         * once its internal state is completely refreshed.
         * </p><p>
         * Note: because the initial population is inherently asynchronous, so it's possible to observe server-side changes
         * (such as a {@link #NODE_UPDATED}) prior to this event being published.
         * </p>
         */
        INITIALIZED
    }

    /**
     * @param type event type
     * @param data event data or null
     */
    public TreeCacheEvent(Type type, ChildData data)
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
        return TreeCacheEvent.class.getSimpleName() + "{" +
            "type=" + type +
            ", data=" + data +
            '}';
    }
}
