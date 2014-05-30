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
