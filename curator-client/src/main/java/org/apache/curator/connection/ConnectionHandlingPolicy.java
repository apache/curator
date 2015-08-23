package org.apache.curator.connection;

import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.RetryLoop;
import java.util.concurrent.Callable;

/**
 * Abstracts connection handling so that Curator can emulate it's old, pre 3.0.0
 * handling and update to newer handling.
 */
public interface ConnectionHandlingPolicy
{
    /**
     * Return true if this policy should behave like the pre-3.0.0 version of Curator
     *
     * @return true/false
     */
    boolean isEmulatingClassicHandling();

    /**
     * Called by {@link RetryLoop#callWithRetry(CuratorZookeeperClient, Callable)} to do the work
     * of retrying
     *
     * @param client client
     * @param proc the procedure to retry
     * @return result
     * @throws Exception errors
     */
    <T> T callWithRetry(CuratorZookeeperClient client, Callable<T> proc) throws Exception;

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
}
