package org.apache.curator.x.rpc.idl.projection;

import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

@ThriftStruct
public class LeaderProjection
{
    @ThriftField(1)
    public String id;

    public LeaderProjection()
    {
    }

    public LeaderProjection(String id)
    {
        this.id = id;
    }
}
