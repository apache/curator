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
package org.apache.curator.framework.api;

public interface CreateBuilder extends CreateBuilderMain
{
    /**
     * Specify a TTL when mode is {@link org.apache.zookeeper.CreateMode#PERSISTENT_WITH_TTL} or
     * {@link org.apache.zookeeper.CreateMode#PERSISTENT_SEQUENTIAL_WITH_TTL}. If
     * the znode has not been modified within the given TTL, it will be deleted once it has no
     * children. The TTL unit is milliseconds and must be greater than 0 and less than or equal to
     * EphemeralType.MAX_TTL.
     *
     * @param ttl the ttl
     * @return this for chaining
     */
    CreateBuilderMain withTtl(long ttl);

    /**
     * If the ZNode already exists, Curator will instead call setData()
     */
    CreateBuilder2 orSetData();

    /**
     * If the ZNode already exists, Curator will instead call setData()
     *
     * @param version the version to use for {@link org.apache.curator.framework.CuratorFramework#setData()}
     */
    CreateBuilder2 orSetData(int version);
}
