package org.apache.curator.x.rpc.idl.projection;

import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

@ThriftStruct
public class PathChildrenCacheProjection
{
    @ThriftField(1)
    public GenericProjection projection;

    public PathChildrenCacheProjection()
    {
    }

    public PathChildrenCacheProjection(GenericProjection projection)
    {
        this.projection = projection;
    }
}
