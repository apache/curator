package org.apache.curator.x.rpc.idl.event;

import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

@ThriftStruct("LeaderEvent")
public class LeaderEvent
{
    @ThriftField(1)
    public String path;

    @ThriftField(2)
    public String participantId;

    @ThriftField(3)
    public boolean isLeader;

    public LeaderEvent()
    {
    }

    public LeaderEvent(String path, String participantId, boolean isLeader)
    {
        this.path = path;
        this.participantId = participantId;
        this.isLeader = isLeader;
    }
}
