package org.apache.curator.connection;

import org.apache.curator.CuratorZookeeperClient;
import org.apache.zookeeper.KeeperException;
import java.util.concurrent.Callable;

public interface ConnectionHandlingPolicy
{
    /**
     * Return true if this policy should behave like the pre-3.0.0 version of Curator
     *
     * @return true/false
     */
    boolean isEmulatingClassicHandling();

    enum CheckTimeoutsResult
    {
        /**
         * Do nothing
         */
        NOP,

        /**
         * handle a new connection string
         */
        NEW_CONNECTION_STRING,

        /**
         * reset/recreate the internal ZooKeeper connection
         */
        RESET_CONNECTION,

        /**
         * handle a connection timeout
         */
        CONNECTION_TIMEOUT,

        /**
         * handle a session timeout
         */
        SESSION_TIMEOUT
    }

    /**
     * Check timeouts. NOTE: this method is only called when an attempt to access to the ZooKeeper instances
     * is made and the connection has not completed.
     *
     * @param hasNewConnectionString proc to call to check if there is a new connection string. Important: the internal state is cleared after
     *                               this is called so you MUST handle the new connection string if <tt>true</tt> is returned
     * @param connectionStartMs the epoch/ms time that the connection was first initiated
     * @param sessionTimeoutMs the configured session timeout in milliseconds
     * @param connectionTimeoutMs the configured connection timeout in milliseconds
     * @return result
     * @throws Exception errors
     */
    CheckTimeoutsResult checkTimeouts(Callable<Boolean> hasNewConnectionString, long connectionStartMs, int sessionTimeoutMs, int connectionTimeoutMs) throws Exception;

    enum PreRetryResult
    {
        /**
         * The retry loop should call the procedure
         */
        CALL_PROC,

        /**
         * Wait again for connection success or timeout
         */
        WAIT_FOR_CONNECTION,

        /**
         * Do not call the procedure and exit the retry loop
         */
        EXIT_RETRIES,

        /**
         * Do not call the procedure and throw {@link KeeperException.ConnectionLossException}
         */
        CONNECTION_TIMEOUT
    }

    /**
     * Called prior to each iteration of a procedure in a retry loop
     *
     * @param client the client
     * @return result
     * @throws Exception errors
     */
    PreRetryResult preRetry(CuratorZookeeperClient client) throws Exception;
}
