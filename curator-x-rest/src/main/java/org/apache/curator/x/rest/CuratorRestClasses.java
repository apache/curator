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
package org.apache.curator.x.rest;

import com.google.common.collect.ImmutableList;
import org.apache.curator.x.rest.api.ClientResource;
import org.apache.curator.x.rest.api.LeaderResource;
import org.apache.curator.x.rest.api.LockResource;
import org.apache.curator.x.rest.api.NodeCacheResource;
import org.apache.curator.x.rest.api.PathChildrenCacheResource;
import org.apache.curator.x.rest.api.PersistentEphemeralNodeResource;
import org.apache.curator.x.rest.api.ReadWriteLockResource;
import org.apache.curator.x.rest.api.SemaphoreResource;
import org.apache.curator.x.rest.api.SessionResource;
import java.util.List;

public class CuratorRestClasses
{
    public static List<Class<?>> getClasses()
    {
        ImmutableList.Builder<Class<?>> builder = ImmutableList.builder();
        builder.add(SessionResource.class);
        builder.add(ClientResource.class);
        builder.add(LockResource.class);
        builder.add(SemaphoreResource.class);
        builder.add(PathChildrenCacheResource.class);
        builder.add(NodeCacheResource.class);
        builder.add(LeaderResource.class);
        builder.add(ReadWriteLockResource.class);
        builder.add(PersistentEphemeralNodeResource.class);
        return builder.build();
    }

    private CuratorRestClasses()
    {
    }
}
