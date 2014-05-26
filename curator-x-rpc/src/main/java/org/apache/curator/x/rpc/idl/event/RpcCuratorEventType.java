package org.apache.curator.x.rpc.idl.event;

import com.facebook.swift.codec.ThriftEnum;

@ThriftEnum("CuratorEventType")
public enum RpcCuratorEventType
{
    CREATE,
    DELETE,
    EXISTS,
    GET_DATA,
    SET_DATA,
    CHILDREN,
    SYNC,
    GET_ACL,
    SET_ACL,
    WATCHED,
    CLOSING
}
