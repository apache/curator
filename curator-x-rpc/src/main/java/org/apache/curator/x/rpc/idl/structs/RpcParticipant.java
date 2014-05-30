package org.apache.curator.x.rpc.idl.structs;

import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

@ThriftStruct("Participant")
public class RpcParticipant
{
    @ThriftField(1)
    public String id;

    @ThriftField(2)
    public boolean isLeader;

    public RpcParticipant()
    {
    }

    public RpcParticipant(String id, boolean isLeader)
    {
        this.id = id;
        this.isLeader = isLeader;
    }
}
