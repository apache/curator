package org.apache.curator.framework.imps;

interface InternalConnectionHandler
{
    void checkNewConnection(CuratorFrameworkImpl client);

    void suspendConnection(CuratorFrameworkImpl client);

    boolean checkSessionExpirationEnabled();
}
