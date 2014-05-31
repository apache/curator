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
package org.apache.curator.x.rpc.idl.services;

import com.facebook.swift.service.ThriftMethod;
import com.facebook.swift.service.ThriftService;
import org.apache.curator.x.rpc.connections.CuratorEntry;
import org.apache.curator.x.rpc.connections.ConnectionManager;
import org.apache.curator.x.rpc.idl.exceptions.RpcException;
import org.apache.curator.x.rpc.idl.structs.CuratorProjection;
import org.apache.curator.x.rpc.idl.structs.RpcCuratorEvent;

@ThriftService("EventService")
public class EventService
{
    private final ConnectionManager connectionManager;
    private final long pingTimeMs;

    public EventService(ConnectionManager connectionManager, long pingTimeMs)
    {
        this.connectionManager = connectionManager;
        this.pingTimeMs = pingTimeMs;
    }

    @ThriftMethod
    public RpcCuratorEvent getNextEvent(CuratorProjection projection) throws RpcException
    {
        try
        {
            CuratorEntry entry = CuratorEntry.mustGetEntry(connectionManager, projection);
            RpcCuratorEvent event = entry.pollForEvent(pingTimeMs);
            return (event != null) ? event : new RpcCuratorEvent();
        }
        catch ( InterruptedException e )
        {
            throw new RpcException(e);
        }
    }
}
