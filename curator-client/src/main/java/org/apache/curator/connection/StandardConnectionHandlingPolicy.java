package org.apache.curator.connection;

import org.apache.curator.CuratorZookeeperClient;
import java.util.concurrent.Callable;

public class StandardConnectionHandlingPolicy implements ConnectionHandlingPolicy
{
    @Override
    public boolean isEmulatingClassicHandling()
    {
        return false;
    }

    @Override
    public CheckTimeoutsResult checkTimeouts(Callable<Boolean> hasNewConnectionString, long connectionStartMs, int sessionTimeoutMs, int connectionTimeoutMs) throws Exception
    {
        if ( hasNewConnectionString.call() )
        {
            return CheckTimeoutsResult.NEW_CONNECTION_STRING;
        }
        return CheckTimeoutsResult.NOP;
    }

    @Override
    public PreRetryResult preRetry(CuratorZookeeperClient client) throws Exception
    {
        // TODO - see if there are other servers to connect to
        if ( !client.isConnected() )
        {
            return PreRetryResult.CONNECTION_TIMEOUT;
        }

        return PreRetryResult.CALL_PROC;
    }
}
