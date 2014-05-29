package org.apache.curator.x.rpc.idl.event;

import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import org.apache.curator.x.rpc.idl.projection.GenericProjection;

@ThriftStruct("LeaderResult")
public class LeaderResult
{
    @ThriftField(1)
    public GenericProjection projection;

    @ThriftField(2)
    public boolean hasLeadership;

    public LeaderResult()
    {
    }

    public LeaderResult(GenericProjection projection, boolean hasLeadership)
    {
        this.projection = projection;
        this.hasLeadership = hasLeadership;
    }
}
