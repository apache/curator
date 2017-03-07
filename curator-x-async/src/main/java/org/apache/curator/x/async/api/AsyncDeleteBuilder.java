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
import java.util.Set;

/**
 * Builder for ZNode deletions
 */
public interface AsyncDeleteBuilder extends AsyncPathable<AsyncStage<Void>>
{
    /**
     * Changes the deletion options. By default, no options are used
     *
     * @param options set of deletion options
     * @return this
     */
    AsyncPathable<AsyncStage<Void>> withOptions(Set<DeleteOption> options);

    /**
     * Set options and version.
     *
     * @param options set of deletion options
     * @param version version to use
     * @see #withOptions(java.util.Set)
     * @see #withVersion(int)
     * @return this
     */
    AsyncPathable<AsyncStage<Void>> withOptionsAndVersion(Set<DeleteOption> options, int version);

    /**
     * Changes the version number passed to the delete() method. By default, -1 is used
     *
     * @param version version to use
     * @return this
     */
    AsyncPathable<AsyncStage<Void>> withVersion(int version);
}
