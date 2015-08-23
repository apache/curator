package org.apache.curator.connection;

import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.RetryLoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.Callable;

public class StandardConnectionHandlingPolicy implements ConnectionHandlingPolicy
{
    private final Logger log = LoggerFactory.getLogger(getClass());

    @Override
    public boolean isEmulatingClassicHandling()
    {
        return false;
    }

    @Override
    public <T> T callWithRetry(CuratorZookeeperClient client, Callable<T> proc) throws Exception
    {
        client.internalBlockUntilConnectedOrTimedOut();

        T result = null;
        RetryLoop retryLoop = client.newRetryLoop();
        while ( retryLoop.shouldContinue() )
        {
            try
            {
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
        if ( hasNewConnectionString.call() )
        {
            return CheckTimeoutsResult.NEW_CONNECTION_STRING;
        }
        return CheckTimeoutsResult.NOP;
    }
}
