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

import java.util.Collection;

/**
 * Adds commit to the transaction interface
 */
public interface CuratorTransactionFinal extends CuratorTransaction
{
    /**
     * Commit all added operations as an atomic unit and return results
     * for the operations. One result is returned for each operation added.
     * Further, the ordering of the results matches the ordering that the
     * operations were added.
     *
     * @return operation results
     * @throws Exception errors
     */
    public Collection<CuratorTransactionResult> commit() throws Exception;
}
