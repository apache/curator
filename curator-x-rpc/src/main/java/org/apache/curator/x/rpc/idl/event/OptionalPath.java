package org.apache.curator.x.rpc.idl.event;

import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

@ThriftStruct
public class OptionalPath
{
    @ThriftField(1)
    public String path;

    public OptionalPath()
    {
    }

    public OptionalPath(String path)
    {
        this.path = path;
    }
}
