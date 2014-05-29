package org.apache.curator.x.rpc.idl.event;

import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;

@ThriftStruct("PathChildrenCacheEvent")
public class RpcPathChildrenCacheEvent
{
    @ThriftField(1)
    public String cachedPath;

    @ThriftField(2)
    public RpcPathChildrenCacheEventType type;

    @ThriftField(3)
    public RpcChildData data;

    public RpcPathChildrenCacheEvent()
    {
    }

    public RpcPathChildrenCacheEvent(String cachedPath, PathChildrenCacheEvent event)
    {
        this.cachedPath = cachedPath;
        type = RpcPathChildrenCacheEventType.valueOf(event.getType().name());
        data = (event.getData() != null) ? new RpcChildData(event.getData().getPath(), RpcCuratorEvent.toRpcStat(event.getData().getStat()), event.getData().getData()) : null;
    }

    public RpcPathChildrenCacheEvent(String cachedPath, RpcPathChildrenCacheEventType type, RpcChildData data)
    {
        this.cachedPath = cachedPath;
        this.type = type;
        this.data = data;
    }
}
