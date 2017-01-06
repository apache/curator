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
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import java.util.List;
import java.util.Set;

/**
 * Builder for ZNode creates
 */
public interface AsyncCreateBuilder extends AsyncPathAndBytesable<AsyncStage<String>>
{
    /**
     * Have the operation fill the provided stat object
     *
     * @param stat the stat to have filled in
     * @return this
     */
    AsyncPathAndBytesable<AsyncStage<String>> storingStatIn(Stat stat);

    /**
     * Use the given create mode. The default is {@link org.apache.zookeeper.CreateMode#PERSISTENT}
     *
     * @param createMode mode to use
     * @return this
     */
    AsyncPathAndBytesable<AsyncStage<String>> withMode(CreateMode createMode);

    /**
     * Set an ACL list (default is {@link org.apache.zookeeper.ZooDefs.Ids#OPEN_ACL_UNSAFE})
     *
     * @param aclList the ACL list to use
     * @return this
     */
    AsyncPathAndBytesable<AsyncStage<String>> withACL(List<ACL> aclList);

    /**
     * Options to change how the ZNode is created
     *
     * @param options options
     * @return this
     */
    AsyncPathAndBytesable<AsyncStage<String>> withOptions(Set<CreateOption> options);

    /**
     * set options and ACLs
     *
     * @param options options
     * @param aclList the ACL list to use
     * @see #withOptions(java.util.Set)
     * @see #withACL(java.util.List)
     * @return this
     */
    AsyncPathAndBytesable<AsyncStage<String>> withOptions(Set<CreateOption> options, List<ACL> aclList);

    /**
     * set options, mode and ACLs
     *
     * @param options options
     * @param createMode mode to use
     * @param aclList the ACL list to use
     * @see #withACL(java.util.List)
     * @see #withOptions(java.util.Set)
     * @see #withMode(org.apache.zookeeper.CreateMode)
     * @see #withACL(java.util.List)
     * @return this
     */
    AsyncPathAndBytesable<AsyncStage<String>> withOptions(Set<CreateOption> options, CreateMode createMode, List<ACL> aclList);

    /**
     * set options and mode
     *
     * @param options options
     * @param createMode mode to use
     * @see #withOptions(java.util.Set)
     * @see #withMode(org.apache.zookeeper.CreateMode)
     * @return this
     */
    AsyncPathAndBytesable<AsyncStage<String>> withOptions(Set<CreateOption> options, CreateMode createMode);

    /**
     * set options, mode, ACLs, and stat
     *
     * @param options options
     * @param createMode mode to use
     * @param aclList the ACL list to use
     * @param stat the stat to have filled in
     * @see #withOptions(java.util.Set)
     * @see #withMode(org.apache.zookeeper.CreateMode)
     * @see #withACL(java.util.List)
     * @see #storingStatIn(org.apache.zookeeper.data.Stat)
     * @return this
     */
    AsyncPathAndBytesable<AsyncStage<String>> withOptions(Set<CreateOption> options, CreateMode createMode, List<ACL> aclList, Stat stat);
}
