package org.apache.curator.x.rpc.idl.structs;

import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

@ThriftStruct
public class Version
{
    @ThriftField(1)
    public int version;

    public Version()
    {
    }

    public Version(int version)
    {
        this.version = version;
    }
}
