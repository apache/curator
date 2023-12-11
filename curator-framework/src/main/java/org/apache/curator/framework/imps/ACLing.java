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

package org.apache.curator.framework.imps;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.utils.InternalACLProvider;
import org.apache.zookeeper.data.ACL;

class ACLing implements InternalACLProvider {
    private final List<ACL> aclList;
    private final ACLProvider aclProvider;
    private final boolean applyToParents;

    ACLing(ACLProvider aclProvider) {
        this(aclProvider, null);
    }

    ACLing(ACLProvider aclProvider, List<ACL> aclList) {
        this(aclProvider, aclList, false);
    }

    ACLing(ACLProvider aclProvider, List<ACL> aclList, boolean applyToParents) {
        this.aclProvider = aclProvider;
        this.aclList = (aclList != null) ? ImmutableList.copyOf(aclList) : null;
        this.applyToParents = applyToParents;
    }

    InternalACLProvider getACLProviderForParents() {
        return applyToParents ? this : aclProvider;
    }

    List<ACL> getAclList(String path) {
        if (aclList != null) return aclList;
        if (path != null) {
            List<ACL> localAclList = aclProvider.getAclForPath(path);
            if (localAclList != null) {
                return localAclList;
            }
        }
        return aclProvider.getDefaultAcl();
    }

    @Override
    public List<ACL> getDefaultAcl() {
        return aclProvider.getDefaultAcl();
    }

    @Override
    public List<ACL> getAclForPath(String path) {
        return getAclList(path);
    }
}
