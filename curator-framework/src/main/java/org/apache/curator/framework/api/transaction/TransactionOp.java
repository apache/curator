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

package org.apache.curator.framework.api.transaction;

import org.apache.curator.framework.CuratorFramework;

/**
 * Builds operations that can be committed as a transaction
 * via {@link CuratorFramework#transaction()}
 */
public interface TransactionOp
{
    /**
     * Start a create builder in the transaction
     *
     * @return builder object
     */
    TransactionCreateBuilder<CuratorOp> create();

    /**
     * Start a delete builder in the transaction
     *
     * @return builder object
     */
    TransactionDeleteBuilder<CuratorOp> delete();

    /**
     * Start a setData builder in the transaction
     *
     * @return builder object
     */
    TransactionSetDataBuilder<CuratorOp> setData();

    /**
     * Start a check builder in the transaction
     *
     * @return builder object
     */
    TransactionCheckBuilder<CuratorOp> check();
}
