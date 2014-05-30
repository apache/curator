package org.apache.curator.x.rpc.idl.structs;

import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

@ThriftStruct
public class PathChildrenCacheProjection
{
    @ThriftField(1)
    public String id;

    public PathChildrenCacheProjection()
    {
    }

    public PathChildrenCacheProjection(String id)
    {
        this.id = id;
    }
}
