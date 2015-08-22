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

import org.apache.curator.drivers.TracerDriver;
import org.apache.curator.utils.DebugUtils;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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
 */
public class RetryLoop
{
    private boolean         isDone = false;
    private int             retryCount = 0;

    private final Logger            log = LoggerFactory.getLogger(getClass());
    private final long              startTimeMs = System.currentTimeMillis();
    private final RetryPolicy       retryPolicy;
    private final AtomicReference<TracerDriver>     tracer;

    private static final RetrySleeper  sleeper = new RetrySleeper()
    {
        @Override
        public void sleepFor(long time, TimeUnit unit) throws InterruptedException
        {
            unit.sleep(time);
        }
    };

    /**
     * Returns the default retry sleeper
     *
     * @return sleeper
     */
    public static RetrySleeper      getDefaultRetrySleeper()
    {
        return sleeper;
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
    public static<T> T      callWithRetry(CuratorZookeeperClient client, Callable<T> proc) throws Exception
    {
        T               result = null;
        RetryLoop       retryLoop = client.newRetryLoop();
        boolean         connectionFailed = false;
        while ( retryLoop.shouldContinue() )
        {
            try
            {
                Exception debugException = client.getDebugException();
                if ( debugException != null )
                {
                    throw debugException;
                }

                client.internalBlockUntilConnectedOrTimedOut();
                if ( !client.isConnected() && !client.retryConnectionTimeouts() )
                {
                    connectionFailed = true;
                    break;
                }

                result = proc.call();
                retryLoop.markComplete();
            }
            catch ( Exception e )
            {
                retryLoop.takeException(e);
            }
        }

        if ( connectionFailed )
        {
            throw new KeeperException.ConnectionLossException();
        }

        return result;
    }

    RetryLoop(RetryPolicy retryPolicy, AtomicReference<TracerDriver> tracer)
    {
        this.retryPolicy = retryPolicy;
        this.tracer = tracer;
    }

    /**
     * If true is returned, make an attempt at the operation
     *
     * @return true/false
     */
    public boolean      shouldContinue()
    {
        return !isDone;
    }

    /**
     * Call this when your operation has successfully completed
     */
    public void     markComplete()
    {
        isDone = true;
    }

    /**
     * Utility - return true if the given Zookeeper result code is retry-able
     *
     * @param rc result code
     * @return true/false
     */
    public static boolean      shouldRetry(int rc)
    {
        return (rc == KeeperException.Code.CONNECTIONLOSS.intValue()) ||
            (rc == KeeperException.Code.OPERATIONTIMEOUT.intValue()) ||
            (rc == KeeperException.Code.SESSIONMOVED.intValue()) ||
            (rc == KeeperException.Code.SESSIONEXPIRED.intValue()) ||
            (rc == KeeperException.Code.NEWCONFIGNOQUORUM.intValue());
    }

    /**
     * Utility - return true if the given exception is retry-able
     *
     * @param exception exception to check
     * @return true/false
     */
    public static boolean      isRetryException(Throwable exception)
    {
        if ( exception instanceof KeeperException )
        {
            KeeperException     keeperException = (KeeperException)exception;
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
    public void         takeException(Exception exception) throws Exception
    {
        boolean     rethrow = true;
        if ( isRetryException(exception) )
        {
            if ( !Boolean.getBoolean(DebugUtils.PROPERTY_DONT_LOG_CONNECTION_ISSUES) )
            {
                log.debug("Retry-able exception received", exception);
            }

            if ( retryPolicy.allowRetry(retryCount++, System.currentTimeMillis() - startTimeMs, sleeper) )
            {
                tracer.get().addCount("retries-allowed", 1);
                if ( !Boolean.getBoolean(DebugUtils.PROPERTY_DONT_LOG_CONNECTION_ISSUES) )
                {
                    log.debug("Retrying operation");
                }
                rethrow = false;
            }
            else
            {
                tracer.get().addCount("retries-disallowed", 1);
                if ( !Boolean.getBoolean(DebugUtils.PROPERTY_DONT_LOG_CONNECTION_ISSUES) )
                {
                    log.debug("Retry policy not allowing retry");
                }
            }
        }

        if ( rethrow )
        {
            throw exception;
        }
    }
}
