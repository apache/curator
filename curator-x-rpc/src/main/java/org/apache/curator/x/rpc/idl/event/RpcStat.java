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
package org.apache.curator.x.rpc.idl.event;

import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

@ThriftStruct("Stat")
public class RpcStat
{
    @ThriftField(1)
    public long czxid;

    @ThriftField(2)
    public long mzxid;

    @ThriftField(3)
    public long ctime;

    @ThriftField(4)
    public long mtime;

    @ThriftField(5)
    public int version;

    @ThriftField(6)
    public int cversion;

    @ThriftField(7)
    public int aversion;

    @ThriftField(8)
    public long ephemeralOwner;

    @ThriftField(9)
    public int dataLength;

    @ThriftField(10)
    public int numChildren;

    @ThriftField(11)
    public long pzxid;

    public RpcStat()
    {
    }

    public RpcStat(long czxid, long mzxid, long ctime, long mtime, int version, int cversion, int aversion, long ephemeralOwner, int dataLength, int numChildren, long pzxid)
    {
        this.czxid = czxid;
        this.mzxid = mzxid;
        this.ctime = ctime;
        this.mtime = mtime;
        this.version = version;
        this.cversion = cversion;
        this.aversion = aversion;
        this.ephemeralOwner = ephemeralOwner;
        this.dataLength = dataLength;
        this.numChildren = numChildren;
        this.pzxid = pzxid;
    }
}
