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
 * Builds operations that can be committed as a transaction
 * via {@link org.apache.curator.x.async.AsyncCuratorFramework#transaction()}
 */
public interface AsyncTransactionOp
{
    /**
     * Start a create builder in the transaction
     *
     * @return builder object
     */
    AsyncTransactionCreateBuilder create();

    /**
     * Start a delete builder in the transaction
     *
     * @return builder object
     */
    AsyncTransactionDeleteBuilder delete();

    /**
     * Start a setData builder in the transaction
     *
     * @return builder object
     */
    AsyncTransactionSetDataBuilder setData();

    /**
     * Start a check builder in the transaction
     *
     * @return builder object
     */
    AsyncTransactionCheckBuilder check();
}
