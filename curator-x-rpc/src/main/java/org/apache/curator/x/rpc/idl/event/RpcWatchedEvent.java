package org.apache.curator.x.rpc.idl.event;

import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

@ThriftStruct("WatchedEvent")
public class RpcWatchedEvent
{
    private RpcKeeperState keeperState;
    private RpcEventType eventType;
    private String path;

    public RpcWatchedEvent()
    {
    }

    public RpcWatchedEvent(RpcKeeperState keeperState, RpcEventType eventType, String path)
    {
        this.keeperState = keeperState;
        this.eventType = eventType;
        this.path = path;
    }

    @ThriftField(1)
    public RpcKeeperState getKeeperState()
    {
        return keeperState;
    }

    public void setKeeperState(RpcKeeperState keeperState)
    {
        this.keeperState = keeperState;
    }

    @ThriftField(2)
    public RpcEventType getEventType()
    {
        return eventType;
    }

    public void setEventType(RpcEventType eventType)
    {
        this.eventType = eventType;
    }

    @ThriftField(3)
    public String getPath()
    {
        return path;
    }

    public void setPath(String path)
    {
        this.path = path;
    }
}
