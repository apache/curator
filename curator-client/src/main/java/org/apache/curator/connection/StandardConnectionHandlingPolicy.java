package org.apache.curator.connection;

import com.google.common.base.Splitter;
import org.apache.curator.CuratorZookeeperClient;
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
        if ( !client.isConnected() )
        {
            int serverCount = Splitter.on(",").omitEmptyStrings().splitToList(client.getCurrentConnectionString()).size();
            if ( serverCount > 1 )
            {
                log.info("Connection timed out and connection string is > 1. Resetting connection and trying again.");
                client.reset(); // unfortunately, there's no way to guarantee that ZK tries a different server. Internally it calls Collections.shuffle(). Hopefully, this will result in a different server each time.
                return PreRetryResult.WAIT_FOR_CONNECTION;
            }
            return PreRetryResult.CONNECTION_TIMEOUT;
        }

        return PreRetryResult.CALL_PROC;
    }
}
