package org.apache.curator.x.rpc.idl.structs;

import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

@ThriftStruct
public class LeaseProjection
{
    @ThriftField(1)
    public String id;

    public LeaseProjection()
    {
    }

    public LeaseProjection(String id)
    {
        this.id = id;
    }
}
