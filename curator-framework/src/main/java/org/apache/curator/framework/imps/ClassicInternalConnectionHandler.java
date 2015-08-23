package org.apache.curator.framework.imps;

import org.apache.curator.framework.state.ConnectionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ClassicInternalConnectionHandler implements InternalConnectionHandler
{
    private final Logger log = LoggerFactory.getLogger(getClass());

    @Override
    public void checkNewConnection(CuratorFrameworkImpl client)
    {
        // NOP
    }

    @Override
    public boolean checkSessionExpirationEnabled()
    {
        return false;
    }

    @Override
    public void suspendConnection(CuratorFrameworkImpl client)
    {
        if ( client.setToSuspended() )
        {
            doSyncForSuspendedConnection(client, client.getZookeeperClient().getInstanceIndex());
        }
    }

    private void doSyncForSuspendedConnection(final CuratorFrameworkImpl client, final long instanceIndex)
    {
        // we appear to have disconnected, force a new ZK event and see if we can connect to another server
        final BackgroundOperation<String> operation = new BackgroundSyncImpl(client, null);
        OperationAndData.ErrorCallback<String> errorCallback = new OperationAndData.ErrorCallback<String>()
        {
            @Override
            public void retriesExhausted(OperationAndData<String> operationAndData)
            {
                // if instanceIndex != newInstanceIndex, the ZooKeeper instance was reset/reallocated
                // so the pending background sync is no longer valid.
                // if instanceIndex is -1, this is the second try to sync - punt and mark the connection lost
                if ( (instanceIndex < 0) || (instanceIndex == client.getZookeeperClient().getInstanceIndex()) )
                {
                    client.addStateChange(ConnectionState.LOST);
                }
                else
                {
                    log.debug("suspendConnection() failure ignored as the ZooKeeper instance was reset. Retrying.");
                    // send -1 to signal that if it happens again, punt and mark the connection lost
                    doSyncForSuspendedConnection(client, -1);
                }
            }
        };
        client.performBackgroundOperation(new OperationAndData<String>(operation, "/", null, errorCallback, null));
    }
}
