package org.apache.curator.connection;

import org.apache.curator.CuratorZookeeperClient;
import java.util.concurrent.Callable;

public class ClassicConnectionHandlingPolicy implements ConnectionHandlingPolicy
{
    @Override
    public boolean isEmulatingClassicHandling()
    {
        return true;
    }

    @Override
    public CheckTimeoutsResult checkTimeouts(Callable<Boolean> hasNewConnectionString, long connectionStartMs, int sessionTimeoutMs, int connectionTimeoutMs) throws Exception
    {
        CheckTimeoutsResult result = CheckTimeoutsResult.NOP;
        int minTimeout = Math.min(sessionTimeoutMs, connectionTimeoutMs);
        long elapsed = System.currentTimeMillis() - connectionStartMs;
        if ( elapsed >= minTimeout )
        {
            if ( hasNewConnectionString.call() )
            {
                result = CheckTimeoutsResult.NEW_CONNECTION_STRING;
            }
            else
            {
                int maxTimeout = Math.max(sessionTimeoutMs, connectionTimeoutMs);
                if ( elapsed > maxTimeout )
                {
                    result = CheckTimeoutsResult.RESET_CONNECTION;
                }
                else
                {
                    result = CheckTimeoutsResult.CONNECTION_TIMEOUT;
                }
            }
        }

        return result;
    }

    @Override
    public PreRetryResult preRetry(CuratorZookeeperClient client) throws Exception
    {
        return PreRetryResult.CALL_PROC;
    }
}
