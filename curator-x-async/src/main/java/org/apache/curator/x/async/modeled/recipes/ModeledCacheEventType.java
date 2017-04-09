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
package org.apache.curator.x.async.modeled.recipes;

public enum ModeledCacheEventType
{
    /**
     * A child was added to the path
     */
    NODE_ADDED,

    /**
     * A child's data was changed
     */
    NODE_UPDATED,

    /**
     * A child was removed from the path
     */
    NODE_REMOVED,

    /**
     * Called when the connection has changed to {@link org.apache.curator.framework.state.ConnectionState#SUSPENDED}
     */
    CONNECTION_SUSPENDED,

    /**
     * Called when the connection has changed to {@link org.apache.curator.framework.state.ConnectionState#RECONNECTED}
     */
    CONNECTION_RECONNECTED,

    /**
     * Called when the connection has changed to {@link org.apache.curator.framework.state.ConnectionState#LOST}
     */
    CONNECTION_LOST,

    /**
     * Signals that the initial cache has been populated.
     */
    INITIALIZED
}
