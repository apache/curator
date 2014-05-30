package org.apache.curator.x.rpc.idl.structs;

import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

@ThriftStruct
public class NodeCacheProjection
{
    @ThriftField(1)
    public String id;

    public NodeCacheProjection()
    {
    }

    public NodeCacheProjection(String id)
    {
        this.id = id;
    }
}
