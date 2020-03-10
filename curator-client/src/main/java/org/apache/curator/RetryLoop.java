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
package org.apache.curator;

import org.apache.curator.connection.ThreadLocalRetryLoop;
import org.apache.curator.utils.ThreadUtils;
import org.apache.zookeeper.KeeperException;
import java.util.concurrent.Callable;

/**
 * <p>Mechanism to perform an operation on Zookeeper that is safe against
 * disconnections and "recoverable" errors.</p>
 *
 * <p>
 * If an exception occurs during the operation, the RetryLoop will process it,
 * check with the current retry policy and either attempt to reconnect or re-throw
 * the exception
 * </p>
 *
 * Canonical usage:<br>
 * <pre>
 * RetryLoop retryLoop = client.newRetryLoop();
 * while ( retryLoop.shouldContinue() )
 * {
 *     try
 *     {
 *         // do your work
 *         ZooKeeper      zk = client.getZooKeeper();    // it's important to re-get the ZK instance in case there was an error and the instance was re-created
 *
 *         retryLoop.markComplete();
 *     }
 *     catch ( Exception e )
 *     {
 *         retryLoop.takeException(e);
 *     }
 * }
 * </pre>
 *
 * <p>
 *     Note: this an {@code abstract class} instead of an {@code interface} for historical reasons. It was originally a class
 *     and if it becomes an interface we risk {@link java.lang.IncompatibleClassChangeError}s with clients.
 * </p>
 */
public abstract class RetryLoop
{
    /**
     * Returns the default retry sleeper
     *
     * @return sleeper
     */
    public static RetrySleeper getDefaultRetrySleeper()
    {
        return RetryLoopImpl.getRetrySleeper();
    }

    /**
     * Convenience utility: creates a retry loop calling the given proc and retrying if needed
     *
     * @param client Zookeeper
     * @param proc procedure to call with retry
     * @param <T> return type
     * @return procedure result
     * @throws Exception any non-retriable errors
     */
    public static <T> T callWithRetry(CuratorZookeeperClient client, Callable<T> proc) throws Exception
    {
        client.internalBlockUntilConnectedOrTimedOut();

        T result = null;
        ThreadLocalRetryLoop threadLocalRetryLoop = new ThreadLocalRetryLoop();
        RetryLoop retryLoop = threadLocalRetryLoop.getRetryLoop(client::newRetryLoop);
        try
        {
            while ( retryLoop.shouldContinue() )
            {
                try
                {
                    result = proc.call();
                    retryLoop.markComplete();
                }
                catch ( Exception e )
                {
                    ThreadUtils.checkInterrupted(e);
                    retryLoop.takeException(e);
                }
            }
        }
        finally
        {
            threadLocalRetryLoop.release();
        }

        return result;
    }

    /**
     * If true is returned, make an attempt at the operation
     *
     * @return true/false
     */
    public abstract boolean shouldContinue();

    /**
     * Call this when your operation has successfully completed
     */
    public abstract void markComplete();

    /**
     * Utility - return true if the given Zookeeper result code is retry-able
     *
     * @param rc result code
     * @return true/false
     */
    public static boolean shouldRetry(int rc)
    {
        return (rc == KeeperException.Code.CONNECTIONLOSS.intValue()) ||
            (rc == KeeperException.Code.OPERATIONTIMEOUT.intValue()) ||
            (rc == KeeperException.Code.SESSIONMOVED.intValue()) ||
            (rc == KeeperException.Code.SESSIONEXPIRED.intValue()) ||
            (rc == -13); // KeeperException.Code.NEWCONFIGNOQUORUM.intValue()) - using hard coded value for ZK 3.4.x compatibility
    }

    /**
     * Utility - return true if the given exception is retry-able
     *
     * @param exception exception to check
     * @return true/false
     */
    public static boolean isRetryException(Throwable exception)
    {
        if ( exception instanceof KeeperException )
        {
            KeeperException keeperException = (KeeperException)exception;
            return shouldRetry(keeperException.code().intValue());
        }
        return false;
    }

    /**
     * Pass any caught exceptions here
     *
     * @param exception the exception
     * @throws Exception if not retry-able or the retry policy returned negative
     */
    public abstract void takeException(Exception exception) throws Exception;
}
