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

package org.apache.curator.x.crimps.async;

import org.apache.curator.framework.api.*;
import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * Zookeeper framework-style client
 */
public interface AsyncCuratorFrameworkDsl
{
    /**
     * Start a create builder
     *
     * @return builder object
     */
    CreateBuilder create();

    /**
     * Start a delete builder
     *
     * @return builder object
     */
    AsyncDeleteBuilder delete();

    /**
     * Start an exists builder
     * <p>
     * The builder will return a Stat object as if org.apache.zookeeper.ZooKeeper.exists() were called.  Thus, a null
     * means that it does not exist and an actual Stat object means it does exist.
     *
     * @return builder object
     */
    ExistsBuilder checkExists();

    /**
     * Start a get data builder
     *
     * @return builder object
     */
    GetDataBuilder getData();

    /**
     * Start a set data builder
     *
     * @return builder object
     */
    AsyncSetDataBuilder setData();

    /**
     * Start a get children builder
     *
     * @return builder object
     */
    AsyncGetChildrenBuilder<CompletionStage<List<String>>> getChildren();

    /**
     * Start a get ACL builder
     *
     * @return builder object
     */
    GetACLBuilder getACL();

    /**
     * Start a set ACL builder
     *
     * @return builder object
     */
    SetACLBuilder setACL();

    /**
     * Start a reconfig builder
     *
     * @return builder object
     */
    ReconfigBuilder reconfig();

    /**
     * Start a getConfig builder
     *
     * @return builder object
     */
    GetConfigBuilder getConfig();

    /**
     * Start a transaction builder
     *
     * @return builder object
     */
    AsyncMultiTransaction transaction();

    /**
     * Start a sync builder. Note: sync is ALWAYS in the background even
     * if you don't use one of the background() methods
     *
     * @return builder object
     */
    SyncBuilder sync();

    /**
     * Start a remove watches builder.
     * @return builder object
     */
    RemoveWatchesBuilder watches();
}
