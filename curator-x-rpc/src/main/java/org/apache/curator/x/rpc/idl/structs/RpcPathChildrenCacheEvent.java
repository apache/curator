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
package org.apache.curator.x.rpc.idl.structs;

import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;

@ThriftStruct("PathChildrenCacheEvent")
public class RpcPathChildrenCacheEvent
{
    @ThriftField(1)
    public String cachedPath;

    @ThriftField(2)
    public RpcPathChildrenCacheEventType type;

    @ThriftField(3)
    public RpcChildData data;

    public RpcPathChildrenCacheEvent()
    {
    }

    public RpcPathChildrenCacheEvent(String cachedPath, PathChildrenCacheEvent event)
    {
        this.cachedPath = cachedPath;
        type = RpcPathChildrenCacheEventType.valueOf(event.getType().name());
        data = (event.getData() != null) ? new RpcChildData(event.getData()) : null;
    }

    public RpcPathChildrenCacheEvent(String cachedPath, RpcPathChildrenCacheEventType type, RpcChildData data)
    {
        this.cachedPath = cachedPath;
        this.type = type;
        this.data = data;
    }
}
