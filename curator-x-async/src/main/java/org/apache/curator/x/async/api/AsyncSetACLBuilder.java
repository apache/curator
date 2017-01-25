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
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import java.util.List;

/**
 * Builder for setting ACLs
 */
public interface AsyncSetACLBuilder
{
    /**
     * Set the given ACLs
     *
     * @param aclList ACLs to set
     * @return this
     */
    AsyncPathable<AsyncStage<Stat>> withACL(List<ACL> aclList);

    /**
     * Set the given ACLs only if the "a" version matches. By default -1 is used
     * which matches all versions.
     *
     * @param aclList ACLs to set
     * @param version "a" version
     * @see org.apache.zookeeper.data.Stat#getAversion()
     * @return this
     */
    AsyncPathable<AsyncStage<Stat>> withACL(List<ACL> aclList, int version);
}
