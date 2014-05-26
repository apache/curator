package org.apache.curator.x.rpc.idl.event;

import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

@ThriftStruct("Acl")
public class RpcAcl
{
    private int perms;
    private RpcId id;

    public RpcAcl()
    {
    }

    public RpcAcl(int perms, RpcId id)
    {
        this.perms = perms;
        this.id = id;
    }

    @ThriftField(1)
    public int getPerms()
    {
        return perms;
    }

    public void setPerms(int perms)
    {
        this.perms = perms;
    }

    @ThriftField(2)
    public RpcId getId()
    {
        return id;
    }

    public void setId(RpcId id)
    {
        this.id = id;
    }
}
