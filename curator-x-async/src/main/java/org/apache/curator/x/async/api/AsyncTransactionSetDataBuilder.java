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

package org.apache.curator.x.async.api;

import org.apache.curator.framework.api.transaction.CuratorOp;

/**
 * @see AsyncTransactionOp#setData()
 */
public interface AsyncTransactionSetDataBuilder extends AsyncPathAndBytesable<CuratorOp>
{
    /**
     * Changes the version number used. By default, -1 is used
     *
     * @param version version to use
     * @return this
     */
    AsyncPathAndBytesable<CuratorOp> withVersion(int version);

    /**
     * Cause the data to be compressed using the configured compression provider
     *
     * @return this
     */
    AsyncPathAndBytesable<CuratorOp> compressed();

    /**
     * Cause the data to be compressed using the configured compression provider.
     * Also changes the version number used. By default, -1 is used
     *
     * @param version version to use
     * @return this
     */
    AsyncPathAndBytesable<CuratorOp> withVersionCompressed(int version);
}
