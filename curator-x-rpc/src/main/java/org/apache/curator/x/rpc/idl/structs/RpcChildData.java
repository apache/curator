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
import org.apache.curator.framework.recipes.cache.ChildData;

@ThriftStruct("ChildData")
public class RpcChildData
{
    @ThriftField(1)
    public String path;

    @ThriftField(2)
    public RpcStat stat;

    @ThriftField(3)
    public byte[] data;

    public RpcChildData()
    {
    }

    public RpcChildData(ChildData data)
    {
        if ( data != null )
        {
            this.path = data.getPath();
            this.stat = RpcCuratorEvent.toRpcStat(data.getStat());
            this.data = data.getData();
        }
    }

    public RpcChildData(String path, RpcStat stat, byte[] data)
    {
        this.path = path;
        this.stat = stat;
        this.data = data;
    }
}
