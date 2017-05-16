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

import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;

import java.util.List;

public interface ParentACLable<T> extends ACLable<T> {

    /**
     * Set an ACL list (default is {@link ZooDefs.Ids#OPEN_ACL_UNSAFE}).
     *
     * If applyToParents is true, then the aclList is applied to the created parents.
     * Existing parent nodes are not affected.
     *
     * @param aclList the ACL list to use
     * @param applyToParents if true, then the aclList is applied to the created parents.
     * @return this
     */
    T withACL(List<ACL> aclList, boolean applyToParents);

}
