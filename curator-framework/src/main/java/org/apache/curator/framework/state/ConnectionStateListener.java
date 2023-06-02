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

package org.apache.curator.framework.state;

import org.apache.curator.framework.CuratorFramework;

public interface ConnectionStateListener {
    /**
     * Called when there is a state change in the connection
     *
     * @param client the client
     * @param newState the new state
     */
    void stateChanged(CuratorFramework client, ConnectionState newState);

    /**
     * ConnectionStateListener managers set via {@link org.apache.curator.framework.CuratorFrameworkFactory.Builder#connectionStateListenerManagerFactory(ConnectionStateListenerManagerFactory)}
     * are allowed to proxy (etc.) ConnectionStateListeners as needed. If this method returns <code>true</code>
     * the ConnectionStateListener manager must <em>not</em> proxy the listener as it's a vital internal
     * listener used by Curator.
     *
     * @return true/false
     */
    default boolean doNotProxy() {
        return false;
    }
}
