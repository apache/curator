package org.apache.curator.x.rpc.idl.event;

import com.facebook.swift.codec.ThriftEnum;

@ThriftEnum("EventType")
public enum RpcEventType
{
    None,
    NodeCreated,
    NodeDeleted,
    NodeDataChanged,
    NodeChildrenChanged
}
