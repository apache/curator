package org.apache.curator.x.rpc.idl.event;

import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

@ThriftStruct("id")
public class RpcId
{
    private String scheme;
    private String id;

    public RpcId()
    {
    }

    public RpcId(String scheme, String id)
    {
        this.scheme = scheme;
        this.id = id;
    }

    @ThriftField(1)
    public String getScheme()
    {
        return scheme;
    }

    public void setScheme(String scheme)
    {
        this.scheme = scheme;
    }

    @ThriftField(2)
    public String getId()
    {
        return id;
    }

    public void setId(String id)
    {
        this.id = id;
    }
}
