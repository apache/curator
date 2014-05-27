package org.apache.curator.x.rpc.idl.event;

import com.facebook.swift.service.ThriftMethod;
import com.facebook.swift.service.ThriftService;
import org.apache.curator.x.rpc.CuratorEntry;
import org.apache.curator.x.rpc.RpcManager;
import org.apache.curator.x.rpc.idl.projection.CuratorProjection;

@ThriftService("EventService")
public class EventService
{
    private final RpcManager rpcManager;
    private final long pingTimeMs;

    public EventService(RpcManager rpcManager, long pingTimeMs)
    {
        this.rpcManager = rpcManager;
        this.pingTimeMs = pingTimeMs;
    }

    @ThriftMethod
    public RpcCuratorEvent getNextEvent(CuratorProjection projection) throws InterruptedException
    {
        CuratorEntry entry = rpcManager.get(projection.id);
        if ( entry == null )
        {
            // TODO
            return null;
        }
        RpcCuratorEvent event = entry.pollForEvent(pingTimeMs);
        return (event != null) ? event : new RpcCuratorEvent();
    }
}
