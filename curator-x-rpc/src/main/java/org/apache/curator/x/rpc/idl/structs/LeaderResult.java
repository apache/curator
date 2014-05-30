package org.apache.curator.x.rpc.idl.structs;

import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

@ThriftStruct
public class LeaderResult
{
    @ThriftField(1)
    public LeaderProjection projection;

    @ThriftField(2)
    public boolean hasLeadership;

    public LeaderResult()
    {
    }

    public LeaderResult(LeaderProjection projection, boolean hasLeadership)
    {
        this.projection = projection;
        this.hasLeadership = hasLeadership;
    }
}
