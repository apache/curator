package org.apache.curator.x.rpc.idl.projection;

import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

@ThriftStruct
public class GenericProjection
{
    @ThriftField(1)
    public String id;

    public GenericProjection()
    {
    }

    public GenericProjection(String id)
    {
        this.id = id;
    }
}
