package org.apache.curator.x.rpc.idl.event;

import com.facebook.swift.codec.ThriftEnum;

@ThriftEnum("PathChildrenCacheEventType")
public enum RpcPathChildrenCacheEventType
{
    CHILD_ADDED,
    CHILD_UPDATED,
    CHILD_REMOVED,
    CONNECTION_SUSPENDED,
    CONNECTION_RECONNECTED,
    CONNECTION_LOST,
    INITIALIZED
}
