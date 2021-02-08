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

import org.apache.curator.RetryLoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * <p>
 *     Retry loops can easily end up getting nested which can cause exponential calls of the retry policy
 *     (see https://issues.apache.org/jira/browse/CURATOR-559). This utility works around that by using
 *     an internal ThreadLocal to hold a retry loop. E.g. if the retry loop fails anywhere in the chain
 *     of nested calls it will fail for the rest of the nested calls instead.
 * </p>
 *
 * <p>
 *     Example usage:
 *
 * <code><pre>
 * ThreadLocalRetryLoop threadLocalRetryLoop = new ThreadLocalRetryLoop();
 * RetryLoop retryLoop = threadLocalRetryLoop.getRetryLoop(client::newRetryLoop);
 * try
 * {
 *     while ( retryLoop.shouldContinue() )
 *     {
 *         try
 *         {
 *             // do work
 *             retryLoop.markComplete();
 *         }
 *         catch ( Exception e )
 *         {
 *             ThreadUtils.checkInterrupted(e);
 *             retryLoop.takeException(e);
 *         }
 *     }
 * }
 * finally
 * {
 *     threadLocalRetryLoop.release();
 * }
 * </pre></code>
 * </p>
 */
public class ThreadLocalRetryLoop
{
    private static final Logger log = LoggerFactory.getLogger(ThreadLocalRetryLoop.class);
    private static final ThreadLocal<Entry> threadLocal = new ThreadLocal<>();

    private static class Entry
    {
        private final RetryLoop retryLoop;
        private int counter;

        Entry(RetryLoop retryLoop)
        {
            this.retryLoop = retryLoop;
        }
    }

    private static class WrappedRetryLoop extends RetryLoop
    {
        private final RetryLoop retryLoop;
        private Exception takenException;

        public WrappedRetryLoop(RetryLoop retryLoop)
        {
            this.retryLoop = retryLoop;
        }

        @Override
        public boolean shouldContinue()
        {
            return retryLoop.shouldContinue() && (takenException == null);
        }

        @Override
        public void markComplete()
        {
            retryLoop.markComplete();
        }

        @Override
        public void takeException(Exception exception) throws Exception
        {
            if ( takenException != null )
            {
                if ( exception.getClass() != takenException.getClass() )
                {
                    log.error("Multiple exceptions in retry loop", exception);
                }
                throw takenException;
            }
            try
            {
                retryLoop.takeException(exception);
            }
            catch ( Exception e )
            {
                takenException = e;
                throw e;
            }
        }
    }

    /**
     * Call to get the current retry loop. If there isn't one, one is allocated
     * via {@code newRetryLoopSupplier}.
     *
     * @param newRetryLoopSupplier supply a new retry loop when needed. Normally you should use {@link org.apache.curator.CuratorZookeeperClient#newRetryLoop()}
     * @return retry loop to use
     */
    public RetryLoop getRetryLoop(Supplier<RetryLoop> newRetryLoopSupplier)
    {
        Entry entry = threadLocal.get();
        if ( entry == null )
        {
            entry = new Entry(new WrappedRetryLoop(newRetryLoopSupplier.get()));
            threadLocal.set(entry);
        }
        ++entry.counter;
        return entry.retryLoop;
    }

    /**
     * Must be called to release the retry loop. See {@link RetryLoop#callWithRetry(org.apache.curator.CuratorZookeeperClient, java.util.concurrent.Callable)}
     * for an example usage.
     */
    public void release()
    {
        Entry entry = Objects.requireNonNull(threadLocal.get(), "No retry loop was set - unbalanced call to release()");
        if ( --entry.counter <= 0 )
        {
            threadLocal.remove();
        }
    }
}
