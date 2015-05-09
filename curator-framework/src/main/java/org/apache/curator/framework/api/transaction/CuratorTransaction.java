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
package org.apache.curator.framework.api.transaction;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.ZooKeeper;

/**
 * <p>
 *     Transactional/atomic operations. See {@link ZooKeeper#multi(Iterable)} for
 *     details on ZooKeeper transactions.
 * </p>
 *
 * <p>
 *     The general form for this interface is:
 * </p>
 *     <pre>
 *         curator.inTransaction().operation().arguments().forPath(...).
 *             and().more-operations.
 *             and().commit();
 *     </pre>
 *
 * <p>
 *     Here's an example that creates two nodes in a transaction
 * </p>
 *     <pre>
 *         curator.inTransaction().
 *             create().forPath("/path-one", path-one-data).
 *             and().create().forPath("/path-two", path-two-data).
 *             and().commit();
 *     </pre>
 *
 * <p>
 *     <b>Important:</b> the operations are not submitted until
 *     {@link CuratorTransactionFinal#commit()} is called.
 * </p>
 *
 * @deprecated Use {@link CuratorFramework#transaction()}
 */
public interface CuratorTransaction
{
    /**
     * Start a create builder in the transaction
     *
     * @return builder object
     */
    public TransactionCreateBuilder<CuratorTransactionBridge> create();

    /**
     * Start a delete builder in the transaction
     *
     * @return builder object
     */
    public TransactionDeleteBuilder<CuratorTransactionBridge> delete();

    /**
     * Start a setData builder in the transaction
     *
     * @return builder object
     */
    public TransactionSetDataBuilder<CuratorTransactionBridge> setData();

    /**
     * Start a check builder in the transaction
     *ChildData
     * @return builder object
     */
    public TransactionCheckBuilder<CuratorTransactionBridge> check();
}
