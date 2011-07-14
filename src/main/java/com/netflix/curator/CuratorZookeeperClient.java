/*
 Copyright 2011 Netflix, Inc.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
 */
package com.netflix.curator;

import com.google.common.base.Preconditions;
import com.netflix.curator.drivers.LoggingDriver;
import com.netflix.curator.drivers.TracerDriver;
import com.netflix.curator.utils.NullLoggingDriver;
import com.netflix.curator.utils.NullTracerDriver;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A wrapper around Zookeeper that takes care of some low-level housekeeping
 */
public class CuratorZookeeperClient implements Closeable
{
    private final ConnectionState                   state;
    private final AtomicReference<RetryPolicy>      retryPolicy = new AtomicReference<RetryPolicy>();
    private final int                               connectionTimeoutMs;
    private final AtomicBoolean                     started = new AtomicBoolean(false);
    private final AtomicReference<LoggingDriver>    log = new AtomicReference<LoggingDriver>(new NullLoggingDriver());
    private final AtomicReference<TracerDriver>     tracer = new AtomicReference<TracerDriver>(new NullTracerDriver());

    /**
     *
     * @param connectString list of servers to connect to
     * @param sessionTimeoutMs session timeout
     * @param connectionTimeoutMs connection timeout
     * @param watcher default watcher or null
     * @param retryPolicy the retry policy to use
     * @throws IOException ZooKeeper creation errors
     */
    public CuratorZookeeperClient(String connectString, int sessionTimeoutMs, int connectionTimeoutMs, Watcher watcher, RetryPolicy retryPolicy) throws IOException
    {
        Preconditions.checkNotNull(connectString);
        Preconditions.checkNotNull(retryPolicy);

        this.connectionTimeoutMs = connectionTimeoutMs;
        state = new ConnectionState(connectString, sessionTimeoutMs, connectionTimeoutMs, watcher, log, tracer);
        setRetryPolicy(retryPolicy);
    }

    /**
     * Return the managed ZK instance.
     *
     * @return client the client
     * @throws Exception if the connection timeout has elapsed or an exception occurs in a background process
     */
    public ZooKeeper getZooKeeper() throws Exception
    {
        return state.getZooKeeper();
    }

    /**
     * Return a new retry loop. All operations should be performed in a retry loop
     *
     * @return new retry loop
     */
    public RetryLoop newRetryLoop()
    {
        return new RetryLoop(retryPolicy.get(), log);
    }

    /**
     * Returns true if the client is current connected
     *
     * @return true/false
     */
    public boolean isConnected()
    {
        return state.isConnected();
    }

    /**
     * This method blocks until the connection to ZK succeeds. Use with caution. The block
     * will timeout after the connection timeout (as passed to the constructor) has elapsed
     *
     * @return true if the connection succeeded, false if not
     * @throws InterruptedException interrupted while waiting
     */
    public boolean blockUntilConnectedOrTimedOut() throws InterruptedException
    {
        Preconditions.checkArgument(started.get());

        log.get().debug("blockUntilConnectedOrTimedOut() start");
        TimeTrace       trace = startTracer("blockUntilConnectedOrTimedOut");

        long            waitTimeMs = connectionTimeoutMs;
        while ( !state.isConnected() && (waitTimeMs > 0) )
        {
            final CountDownLatch    latch = new CountDownLatch(1);
            Watcher                 previousWatcher = state.substituteParentWatcher
            (
                new Watcher()
                {
                    @Override
                    public void process(WatchedEvent event)
                    {
                        latch.countDown();
                    }
                }
            );

            long        startTimeMs = System.currentTimeMillis();
            try
            {
                latch.await(1, TimeUnit.SECONDS);
            }
            finally
            {
                state.substituteParentWatcher(previousWatcher);
            }
            long        elapsed = Math.max(1, System.currentTimeMillis() - startTimeMs);
            waitTimeMs -= elapsed;
        }

        trace.commit();

        boolean localIsConnected = state.isConnected();
        log.get().debug("blockUntilConnectedOrTimedOut() end. isConnected: " + localIsConnected);

        return localIsConnected;
    }

    /**
     * Must be called after construction
     *
     * @throws IOException errors
     */
    public void     start() throws IOException
    {
        log.get().debug("Starting");

        if ( !started.compareAndSet(false, true) )
        {
            IllegalStateException error = new IllegalStateException();
            log.get().error("Already started", error);
            throw error;
        }

        state.start();
    }

    /**
     * Close the client
     */
    public void     close()
    {
        log.get().debug("Closing");

        started.set(false);
        state.close();
    }

    /**
     * Change the retry policy
     *
     * @param policy new policy
     */
    public void     setRetryPolicy(RetryPolicy policy)
    {
        Preconditions.checkNotNull(policy);

        retryPolicy.set(policy);
    }

    /**
     * Return the current retry policy
     *
     * @return policy
     */
    public RetryPolicy getRetryPolicy()
    {
        return retryPolicy.get();
    }

    /**
     * Returns the current logging driver
     *
     * @return log
     */
    public LoggingDriver      getLog()
    {
        return log.get();
    }

    /**
     * Change the logging driver
     *
     * @param log new logger
     */
    public void               setLog(LoggingDriver log)
    {
        this.log.set(log);
    }

    /**
     * Start a new tracer
     * @param name name of the event
     * @return the new tracer ({@link TimeTrace#commit()} must be called)
     */
    public TimeTrace          startTracer(String name)
    {
        return new TimeTrace(name, tracer.get());
    }

    /**
     * Return the current tracing driver
     *
     * @return tracing driver
     */
    public TracerDriver       getTracerDriver()
    {
        return tracer.get();
    }

    /**
     * Change the tracing driver
     *
     * @param tracer new tracing driver
     */
    public void               setTracerDriver(TracerDriver tracer)
    {
        this.tracer.set(tracer);
    }
}
