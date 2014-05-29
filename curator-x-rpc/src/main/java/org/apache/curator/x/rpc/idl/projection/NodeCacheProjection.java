package org.apache.curator.x.rpc.idl.projection;

import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

@ThriftStruct
public class NodeCacheProjection
{
    @ThriftField(1)
    public GenericProjection projection;

    public NodeCacheProjection()
    {
    }

    public NodeCacheProjection(GenericProjection projection)
    {
        this.projection = projection;
    }
}
