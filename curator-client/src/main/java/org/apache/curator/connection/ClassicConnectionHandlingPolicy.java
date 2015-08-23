package org.apache.curator.connection;

import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.RetryLoop;
import java.util.concurrent.Callable;

/**
 * Emulates the pre 3.0.0 Curator connection handling
 */
public class ClassicConnectionHandlingPolicy implements ConnectionHandlingPolicy
{
    @Override
    public boolean isEmulatingClassicHandling()
    {
        return true;
    }

    @Override
    public <T> T callWithRetry(CuratorZookeeperClient client, Callable<T> proc) throws Exception
    {
        T result = null;
        RetryLoop retryLoop = client.newRetryLoop();
        while ( retryLoop.shouldContinue() )
        {
            try
            {
                client.internalBlockUntilConnectedOrTimedOut();
                result = proc.call();
                retryLoop.markComplete();
            }
            catch ( Exception e )
            {
                retryLoop.takeException(e);
            }
        }

        return result;
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
}
