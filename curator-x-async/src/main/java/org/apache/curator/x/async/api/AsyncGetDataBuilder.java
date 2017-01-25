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

import org.apache.curator.x.async.AsyncStage;
import org.apache.zookeeper.data.Stat;

/**
 * Builder to get ZNode data
 */
public interface AsyncGetDataBuilder extends AsyncPathable<AsyncStage<byte[]>>
{
    /**
     * Cause the data to be de-compressed using the configured compression provider
     *
     * @return this
     */
    AsyncPathable<AsyncStage<byte[]>> decompressed();

    /**
     * Have the operation fill the provided stat object
     *
     * @param stat the stat to have filled in
     * @return this
     */
    AsyncPathable<AsyncStage<byte[]>> storingStatIn(Stat stat);

    /**
     * Have the operation fill the provided stat object and have the data be de-compressed
     *
     * @param stat the stat to have filled in
     * @see #decompressed()
     * @see #storingStatIn(org.apache.zookeeper.data.Stat)
     * @return this
     */
    AsyncPathable<AsyncStage<byte[]>> decompressedStoringStatIn(Stat stat);
}
