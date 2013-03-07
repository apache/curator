/*
 *
 *  Copyright 2011 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package org.apache.curator.framework.imps;

import com.google.common.collect.ImmutableList;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.zookeeper.data.ACL;
import java.util.List;

class ACLing
{
    private final List<ACL>     aclList;
    private final ACLProvider   aclProvider;

    ACLing(ACLProvider aclProvider)
    {
        this(aclProvider, null);
    }

    ACLing(ACLProvider aclProvider, List<ACL> aclList)
    {
        this.aclProvider = aclProvider;
        this.aclList = (aclList != null) ? ImmutableList.copyOf(aclList) : null;
    }

    List<ACL> getAclList(String path)
    {
        List<ACL> localAclList = aclList;
        do
        {
            if ( localAclList != null )
            {
                break;
            }

            if ( path != null )
            {
                localAclList = aclProvider.getAclForPath(path);
                if ( localAclList != null )
                {
                    break;
                }
            }

            localAclList = aclProvider.getDefaultAcl();
        } while ( false );
        return localAclList;
    }
}
