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
package org.apache.curator.x.rpc.idl.projection;

import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

@ThriftStruct
public class CreateSpec
{
    @ThriftField(1)
    public String path;

    @ThriftField(2)
    public byte[] data;

    @ThriftField(3)
    public CreateMode mode;

    @ThriftField(4)
    public boolean doAsync;

    @ThriftField(5)
    public boolean compressed;

    @ThriftField(6)
    public boolean creatingParentsIfNeeded;

    @ThriftField(7)
    public boolean withProtection;

    public CreateSpec()
    {
    }

    public CreateSpec(String path, byte[] data, CreateMode mode, boolean doAsync, boolean compressed, boolean creatingParentsIfNeeded, boolean withProtection)
    {
        this.path = path;
        this.data = data;
        this.mode = mode;
        this.doAsync = doAsync;
        this.compressed = compressed;
        this.creatingParentsIfNeeded = creatingParentsIfNeeded;
        this.withProtection = withProtection;
    }
}
