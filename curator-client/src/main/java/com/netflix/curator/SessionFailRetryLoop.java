/*
 * Copyright 2013 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.curator;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import java.io.Closeable;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * <p>
 *     See {@link RetryLoop} for the main details on retry loops. <b>All Curator/ZooKeeper operations
 *     should be done in a retry loop.</b>
 * </p>
 *
 * <p>
 *     The standard retry loop treats session failure as a type of connection failure. i.e. the fact
 *     that it is a session failure isn't considered. This can be problematic if you are performing
 *     a series of operations that rely on ephemeral nodes. If the session fails after the ephemeral
 *     node has been created, future Curator/ZooKeeper operations may succeed even though the
 *     ephemeral node has been removed by ZooKeeper.
 * </p>
 *
 * <p>
 *     Here's an example:
 *     <ul>
 *         <li>You create an ephemeral/sequential node as a kind of lock/marker</li>
 *         <li>You perform some other operations</li>
 *         <li>The session fails for some reason</li>
 *         <li>You attempt to create a node assuming that the lock/marker still exists</li>
 *         <ul>
 *             <li>Curator will notice the session failure and try to reconnect</li>
 *             <li>In most cases, the reconnect will succeed and, thus, the node creation will succeed
 *             even though the ephemeral node will have been deleted by ZooKeeper.</li>
 *         </ul>
 *     </ul>
 * </p>
 *
 * <p>
 *     The SessionFailRetryLoop prevents this type of scenario. When a session failure is detected,
 *     the thread is marked as failed which will cause all future Curator operations to fail. The
 *     SessionFailRetryLoop will then either retry the entire
 *     set of operations or fail (depending on {@link SessionFailRetryLoop.Mode})
 * </p>
 *
 * Canonical usage:<br/>
 * <code><pre>
 * SessionFailRetryLoop    retryLoop = client.newSessionFailRetryLoop(mode);
 * retryLoop.start();
 * try
 * {
 *     while ( retryLoop.shouldContinue() )
 *     {
 *         try
 *         {
 *             // do work
 *         }
 *         catch ( Exception e )
 *         {
 *             retryLoop.takeException(e);
 *         }
 *     }
 * }
 * finally
 * {
 *     retryLoop.close();
 * }
 * </pre></code>
 */
public class SessionFailRetryLoop implements Closeable
{
    private final CuratorZookeeperClient    client;
    private final Mode                      mode;
    private final Thread                    ourThread = Thread.currentThread();
    private final AtomicBoolean             sessionHasFailed = new AtomicBoolean(false);
    private final AtomicBoolean             isDone = new AtomicBoolean(false);
    private final RetryLoop                 retryLoop;

    private final Watcher         watcher = new Watcher()
    {
        @Override
        public void process(WatchedEvent event)
        {
            if ( event.getState() == Event.KeeperState.Expired )
            {
                sessionHasFailed.set(true);
                failedSessionThreads.add(ourThread);
            }
        }
    };

    private static final Set<Thread>        failedSessionThreads = Sets.newSetFromMap(Maps.<Thread, Boolean>newConcurrentMap());

    public static class SessionFailedException extends Exception
    {
    }

    public enum Mode
    {
        /**
         * If the session fails, retry the entire set of operations when {@link SessionFailRetryLoop#shouldContinue()}
         * is called
         */
        RETRY,

        /**
         * If the session fails, throw {@link KeeperException.SessionExpiredException} when
         * {@link SessionFailRetryLoop#shouldContinue()} is called
         */
        FAIL
    }

    /**
     * Convenience utility: creates a "session fail" retry loop calling the given proc
     *
     * @param client Zookeeper
     * @param mode how to handle session failures
     * @param proc procedure to call with retry
     * @param <T> return type
     * @return procedure result
     * @throws Exception any non-retriable errors
     */
    public static<T> T      callWithRetry(CuratorZookeeperClient client, Mode mode, Callable<T> proc) throws Exception
    {
        T                       result = null;
        SessionFailRetryLoop    retryLoop = client.newSessionFailRetryLoop(mode);
        retryLoop.start();
        try
        {
            while ( retryLoop.shouldContinue() )
            {
                try
                {
                    result = proc.call();
                }
                catch ( Exception e )
                {
                    retryLoop.takeException(e);
                }
            }
        }
        finally
        {
            retryLoop.close();
        }
        return result;
    }

    SessionFailRetryLoop(CuratorZookeeperClient client, Mode mode)
    {
        this.client = client;
        this.mode = mode;
        retryLoop = client.newRetryLoop();
    }

    static boolean sessionForThreadHasFailed()
    {
        return (failedSessionThreads.size() > 0) && failedSessionThreads.contains(Thread.currentThread());
    }

    /**
     * SessionFailRetryLoop must be started
     */
    public void     start()
    {
        Preconditions.checkState(Thread.currentThread().equals(ourThread), "Not in the correct thread");

        client.addParentWatcher(watcher);
    }

    /**
     * If true is returned, make an attempt at the set of operations
     *
     * @return true/false
     */
    public boolean      shouldContinue()
    {
        boolean     localIsDone = isDone.getAndSet(true);
        return !localIsDone;
    }

    /**
     * Must be called in a finally handler when done with the loop
     */
    @Override
    public void close()
    {
        Preconditions.checkState(Thread.currentThread().equals(ourThread), "Not in the correct thread");
        failedSessionThreads.remove(ourThread);

        client.removeParentWatcher(watcher);
    }

    /**
     * Pass any caught exceptions here
     *
     * @param exception the exception
     * @throws Exception if not retry-able or the retry policy returned negative
     */
    public void         takeException(Exception exception) throws Exception
    {
        Preconditions.checkState(Thread.currentThread().equals(ourThread), "Not in the correct thread");

        boolean     passUp = true;
        if ( sessionHasFailed.get() )
        {
            switch ( mode )
            {
                case RETRY:
                {
                    sessionHasFailed.set(false);
                    failedSessionThreads.remove(ourThread);
                    if ( exception instanceof SessionFailedException )
                    {
                        isDone.set(false);
                        passUp = false;
                    }
                    break;
                }

                case FAIL:
                {
                    break;
                }
            }
        }

        if ( passUp )
        {
            retryLoop.takeException(exception);
        }
    }
}
