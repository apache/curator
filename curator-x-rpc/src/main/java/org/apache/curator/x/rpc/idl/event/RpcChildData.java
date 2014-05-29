package org.apache.curator.x.rpc.idl.event;

import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import org.apache.curator.framework.recipes.cache.ChildData;

@ThriftStruct("ChildData")
public class RpcChildData
{
    @ThriftField(1)
    public String path;

    @ThriftField(2)
    public RpcStat stat;

    @ThriftField(3)
    public byte[] data;

    public RpcChildData()
    {
    }

    public RpcChildData(ChildData data)
    {
        if ( data != null )
        {
            this.path = data.getPath();
            this.stat = RpcCuratorEvent.toRpcStat(data.getStat());
            this.data = data.getData();
        }
    }

    public RpcChildData(String path, RpcStat stat, byte[] data)
    {
        this.path = path;
        this.stat = stat;
        this.data = data;
    }
}
