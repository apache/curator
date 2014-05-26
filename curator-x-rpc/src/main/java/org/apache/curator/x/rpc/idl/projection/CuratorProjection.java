package org.apache.curator.x.rpc.idl.projection;

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

@ThriftStruct
public class CuratorProjection
{
    private final String id;

    @ThriftConstructor
    public CuratorProjection(String id)
    {
        this.id = id;
    }

    @ThriftField(1)
    public String getId()
    {
        return id;
    }
}
