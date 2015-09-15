/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
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
     * <p>
     *     Prior to 3.0.0, Curator did not try to manage session expiration
     *     other than the functionality provided by ZooKeeper itself. Starting with
     *     3.0.0, Curator has the option of attempting to monitor session expiration
     *     above what is provided by ZooKeeper. The percentage returned by this method
     *     determines how and if Curator will check for session expiration.
     * </p>
     *
     * <p>
     *     If this method returns <tt>0</tt>, Curator does not
     *     do any additional checking for session expiration.
     * </p>
     *
     * <p>
     *     If a positive number is returned, Curator will check for session expiration
     *     as follows: when ZooKeeper sends a Disconnect event, Curator will start a timer.
     *     If re-connection is not achieved before the elapsed time exceeds the negotiated
     *     session time multiplied by the session expiration percent, Curator will simulate
     *     a session expiration. Due to timing/network issues, it is <b>not possible</b> for
     *     a client to match the server's session timeout with complete accuracy. Thus, the need
     *     for a session expiration percentage.
     * </p>
     *
     * @return a percentage from 0 to 100 (0 implied no extra session checking)
     */
    int getSimulatedSessionExpirationPercent();

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
     * @param sessionTimeoutMs the configured/negotiated session timeout in milliseconds
     * @param connectionTimeoutMs the configured connection timeout in milliseconds
     * @return result
     * @throws Exception errors
     */
    CheckTimeoutsResult checkTimeouts(Callable<Boolean> hasNewConnectionString, long connectionStartMs, int sessionTimeoutMs, int connectionTimeoutMs) throws Exception;
}
