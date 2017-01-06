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
package org.apache.curator.x.async;

/**
 * Zookeeper framework-style client
 */
public interface WatchedAsyncCuratorFramework
{
    /**
     * Start an exists builder
     * <p>
     * The builder will return a Stat object as if org.apache.zookeeper.ZooKeeper.exists() were called.  Thus, a null
     * means that it does not exist and an actual Stat object means it does exist.
     *
     * @return builder object
     */
    AsyncExistsBuilder checkExists();

    /**
     * Start a get data builder
     *
     * @return builder object
     */
    AsyncGetDataBuilder getData();

    /**
     * Start a get children builder
     *
     * @return builder object
     */
    AsyncGetChildrenBuilder getChildren();

    /**
     * Start a getConfig builder
     *
     * @return builder object
     */
    AsyncGetConfigBuilder getConfig();
}
