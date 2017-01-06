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

import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;
import java.util.List;

/**
 * @see org.apache.curator.x.async.AsyncTransactionOp#create()
 */
public interface AsyncTransactionCreateBuilder extends AsyncPathAndBytesable<CuratorOp>
{
    /**
     * Specify a mode for the create. The default is {@link org.apache.zookeeper.CreateMode#PERSISTENT}
     *
     * @param createMode mode
     * @return this
     */
    AsyncPathable<CuratorOp> withMode(CreateMode createMode);

    /**
     * Set an ACL list (default is {@link org.apache.zookeeper.ZooDefs.Ids#OPEN_ACL_UNSAFE})
     *
     * @param aclList the ACL list to use
     * @return this
     */
    AsyncPathable<CuratorOp> withACL(List<ACL> aclList);

    /**
     * Cause the data to be compressed using the configured compression provider
     *
     * @return this
     */
    AsyncPathable<CuratorOp> compressed();

    /**
     * Specify mode, acl list and compression
     *
     * @param createMode mode
     * @param aclList the ACL list to use
     * @param compressed true to compress
     * @see #withMode(org.apache.zookeeper.CreateMode)
     * @see #withACL(java.util.List)
     * @see #compressed()
     * @return this
     */
    AsyncPathable<CuratorOp> withOptions(CreateMode createMode, List<ACL> aclList, boolean compressed);
}
