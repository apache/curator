/*
 * Copyright 2013 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.apache.curator.framework.imps;

import org.apache.curator.framework.api.ACLProvider;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import java.util.List;

public class DefaultACLProvider implements ACLProvider
{
    @Override
    public List<ACL> getDefaultAcl()
    {
        return ZooDefs.Ids.OPEN_ACL_UNSAFE;
    }

    @Override
    public List<ACL> getAclForPath(String path)
    {
        return ZooDefs.Ids.OPEN_ACL_UNSAFE;
    }
}
