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

import org.apache.curator.framework.api.transaction.CuratorOp;

/**
 * Zookeeper framework-style client
 */
public interface AsyncCuratorFrameworkDsl extends WatchedAsyncCuratorFramework
{
    /**
     * Start a create builder
     *
     * @return builder object
     */
    AsyncCreateBuilder create();

    /**
     * Start a delete builder
     *
     * @return builder object
     */
    AsyncDeleteBuilder delete();

    /**
     * Start a set data builder
     *
     * @return builder object
     */
    AsyncSetDataBuilder setData();

    /**
     * Start a get ACL builder
     *
     * @return builder object
     */
    AsyncGetACLBuilder getACL();

    /**
     * Start a set ACL builder
     *
     * @return builder object
     */
    AsyncSetACLBuilder setACL();

    /**
     * Start a reconfig builder
     *
     * @return builder object
     */
    AsyncReconfigBuilder reconfig();

    /**
     * Start a transaction builder
     *
     * @return builder object
     */
    AsyncMultiTransaction transaction();

    /**
     * Allocate an operation that can be used with {@link #transaction()}.
     * NOTE: {@link CuratorOp} instances created by this builder are
     * reusable.
     *
     * @return operation builder
     */
    AsyncTransactionOp transactionOp();

    /**
     * Start a sync builder. Note: sync is ALWAYS in the background even
     * if you don't use one of the background() methods
     *
     * @return builder object
     */
    AsyncSyncBuilder sync();

    /**
     * Start a remove watches builder.
     * @return builder object
     */
    AsyncRemoveWatchesBuilder removeWatches();
}
