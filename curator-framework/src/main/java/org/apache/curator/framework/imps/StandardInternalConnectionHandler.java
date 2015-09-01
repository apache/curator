package org.apache.curator.framework.imps;

class StandardInternalConnectionHandler implements InternalConnectionHandler
{
    @Override
    public void suspendConnection(CuratorFrameworkImpl client)
    {
        client.setToSuspended();
    }

    @Override
    public boolean checkSessionExpirationEnabled()
    {
        return true;
    }

    @Override
    public void checkNewConnection(CuratorFrameworkImpl client)
    {
        client.checkInstanceIndex();
    }
}
