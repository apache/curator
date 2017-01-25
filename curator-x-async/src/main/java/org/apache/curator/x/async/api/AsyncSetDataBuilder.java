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
 * Builder for setting ZNode data
 */
public interface AsyncSetDataBuilder extends AsyncPathAndBytesable<AsyncStage<Stat>>
{
    /**
     * Cause the data to be compressed using the configured compression provider
     *
     * @return this
     */
    AsyncPathAndBytesable<AsyncStage<Stat>> compressed();

    /**
     * Cause the data to be compressed using the configured compression provider.
     * Only sets if the version matches. By default -1 is used
     * which matches all versions.
     *
     * @param version version
     * @return this
     */
    AsyncPathAndBytesable<AsyncStage<Stat>> compressedWithVersion(int version);

    /**
     * Only sets if the version matches. By default -1 is used
     * which matches all versions.
     *
     * @param version version
     * @return this
     */
    AsyncPathAndBytesable<AsyncStage<Stat>> withVersion(int version);
}
