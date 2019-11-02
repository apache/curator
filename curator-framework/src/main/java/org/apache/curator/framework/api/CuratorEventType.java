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
package org.apache.curator.framework.api;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.Watcher;

public enum CuratorEventType
{
    /**
     * Corresponds to {@link CuratorFramework#create()}
     */
    CREATE,

    /**
     * Corresponds to {@link CuratorFramework#delete()}
     */
    DELETE,

    /**
     * Corresponds to {@link CuratorFramework#checkExists()}
     */
    EXISTS,

    /**
     * Corresponds to {@link CuratorFramework#getData()}
     */
    GET_DATA,

    /**
     * Corresponds to {@link CuratorFramework#setData()}
     */
    SET_DATA,

    /**
     * Corresponds to {@link CuratorFramework#getChildren()}
     */
    CHILDREN,

    /**
     * Corresponds to {@link CuratorFramework#sync(String, Object)}
     */
    SYNC,

    /**
     * Corresponds to {@link CuratorFramework#getACL()}
     */
    GET_ACL,

    /**
     * Corresponds to {@link CuratorFramework#setACL()}
     */
    SET_ACL,

    /**
     * Corresponds to {@link CuratorFramework#transaction()}
     */
    TRANSACTION,

    /**
     * Corresponds to {@link CuratorFramework#getConfig()}
     */
    GET_CONFIG,

    /**
     * Corresponds to {@link CuratorFramework#reconfig()}
     */
    RECONFIG,

    /**
     * Corresponds to {@link Watchable#usingWatcher(Watcher)} or {@link Watchable#watched()}
     */
    WATCHED,

    /**
     * Corresponds to {@link CuratorFramework#watches()} ()}
     */
    REMOVE_WATCHES,

    /**
     * Event sent when client is being closed
     */
    CLOSING,

    /**
     * Corresponds to {@link org.apache.curator.framework.CuratorFramework#watchers()}
     */
    ADD_WATCH
}
