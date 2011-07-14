/*
 Copyright 2011 Netflix, Inc.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
 */
package com.netflix.curator.framework.imps;

import com.google.common.collect.ImmutableList;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import java.util.List;

class ACLing
{
    private final List<ACL>     aclList;

    ACLing()
    {
        this(null);
    }

    ACLing(List<ACL> aclList)
    {
        this.aclList = (aclList != null) ? ImmutableList.copyOf(aclList) : ZooDefs.Ids.OPEN_ACL_UNSAFE;
    }

    List<ACL> getAclList()
    {
        return aclList;
    }
}
