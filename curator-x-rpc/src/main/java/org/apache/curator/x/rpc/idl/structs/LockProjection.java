package org.apache.curator.x.rpc.idl.structs;

import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

@ThriftStruct
public class LockProjection
{
    @ThriftField(1)
    public String id;

    public LockProjection()
    {
    }

    public LockProjection(String id)
    {
        this.id = id;
    }
}
