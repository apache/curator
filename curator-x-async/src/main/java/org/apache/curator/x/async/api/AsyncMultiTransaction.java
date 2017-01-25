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
package org.apache.curator.x.async.api;

import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.x.async.AsyncStage;
import java.util.List;

/**
 * Terminal operation to support multi/transactions
 */
public interface AsyncMultiTransaction
{
    /**
     * Invoke ZooKeeper to commit the given operations as a single transaction. Create the
     * operation instances via {@link org.apache.curator.x.async.AsyncCuratorFramework#transactionOp()}
     *
     * @param operations operations that make up the transaction.
     * @return AsyncStage instance for managing the completion
     */
    AsyncStage<List<CuratorTransactionResult>> forOperations(List<CuratorOp> operations);
}
