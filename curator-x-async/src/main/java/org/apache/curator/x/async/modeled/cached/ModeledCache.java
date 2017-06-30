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
package org.apache.curator.x.async.modeled.cached;

import org.apache.curator.x.async.modeled.ZNode;
import org.apache.curator.x.async.modeled.ZPath;
import java.util.Map;
import java.util.Optional;

public interface ModeledCache<T>
{
    /**
     * Return the modeled current data for the given path. There are no guarantees of accuracy. This is
     * merely the most recent view of the data. If there is no node at the given path,
     * {@link java.util.Optional#empty()} is returned.
     *
     * @param path path to the node to check
     * @return data if the node is alive, or empty
     */
    Optional<ZNode<T>> currentData(ZPath path);

    /**
     * Return the modeled current set of children at the given path, mapped by child name. There are no
     * guarantees of accuracy; this is merely the most recent view of the data.
     *
     * @param path path to the node to check
     * @return a possibly-empty map of children if the node is alive
     */
    Map<ZPath, ZNode<T>> currentChildren(ZPath path);
}
