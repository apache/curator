package org.apache.curator.x.rpc.idl.event;

import com.facebook.swift.codec.ThriftEnum;

@ThriftEnum("KeeperState")
public enum RpcKeeperState
{
    Unknown,
    Disconnected,
    NoSyncConnected,
    SyncConnected,
    AuthFailed,
    ConnectedReadOnly,
    SaslAuthenticated,
    Expired
}
