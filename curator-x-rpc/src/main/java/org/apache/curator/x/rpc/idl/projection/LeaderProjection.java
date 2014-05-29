package org.apache.curator.x.rpc.idl.projection;

import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

@ThriftStruct
public class LeaderProjection
{
    @ThriftField(1)
    public GenericProjection projection;

    public LeaderProjection()
    {
    }

    public LeaderProjection(GenericProjection projection)
    {
        this.projection = projection;
    }
}
