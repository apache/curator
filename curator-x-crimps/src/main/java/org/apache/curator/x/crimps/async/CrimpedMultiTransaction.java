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

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import java.util.List;
import java.util.concurrent.CompletionStage;

public interface CrimpedMultiTransaction
{
    /**
     * Commit the given operations as a single transaction. Create the
     * operation instances via {@link CuratorFramework#transactionOp()}
     *
     * @param operations operations that make up the transaction.
     * @return result details for foreground operations or <code>null</code> for background operations
     * @throws Exception errors
     */
    CompletionStage<List<CuratorTransactionResult>> forOperations(List<CuratorOp> operations) throws Exception;
}
