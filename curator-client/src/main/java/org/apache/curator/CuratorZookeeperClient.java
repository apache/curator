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

import com.google.common.base.Preconditions;
import org.apache.curator.connection.ConnectionHandlingPolicy;
import org.apache.curator.connection.StandardConnectionHandlingPolicy;
import org.apache.curator.drivers.OperationTrace;
import org.apache.curator.drivers.TracerDriver;
import org.apache.curator.ensemble.EnsembleProvider;
import org.apache.curator.ensemble.fixed.FixedEnsembleProvider;
import org.apache.curator.utils.DefaultTracerDriver;
import org.apache.curator.utils.DefaultZookeeperFactory;
import org.apache.curator.utils.ThreadUtils;
import org.apache.curator.utils.ZookeeperFactory;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.admin.ZooKeeperAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A wrapper around Zookeeper that takes care of some low-level housekeeping
 */
@SuppressWarnings("UnusedDeclaration")
public class CuratorZookeeperClient implements Closeable
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final ConnectionState state;
    private final AtomicReference<RetryPolicy> retryPolicy = new AtomicReference<RetryPolicy>();
    private final int connectionTimeoutMs;
    private final int waitForShutdownTimeoutMs;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicReference<TracerDriver> tracer = new AtomicReference<TracerDriver>(new DefaultTracerDriver());
    private final ConnectionHandlingPolicy connectionHandlingPolicy;

    /**
     *
     * @param connectString list of servers to connect to
     * @param sessionTimeoutMs session timeout
     * @param connectionTimeoutMs connection timeout
     * @param watcher default watcher or null
     * @param retryPolicy the retry policy to use
     */
    public CuratorZookeeperClient(String connectString, int sessionTimeoutMs, int connectionTimeoutMs, Watcher watcher, RetryPolicy retryPolicy)
    {
        this(new DefaultZookeeperFactory(), new FixedEnsembleProvider(connectString), sessionTimeoutMs, connectionTimeoutMs, watcher, retryPolicy, false, new StandardConnectionHandlingPolicy());
    }

    /**
     * @param ensembleProvider the ensemble provider
     * @param sessionTimeoutMs session timeout
     * @param connectionTimeoutMs connection timeout
     * @param watcher default watcher or null
     * @param retryPolicy the retry policy to use
     */
    public CuratorZookeeperClient(EnsembleProvider ensembleProvider, int sessionTimeoutMs, int connectionTimeoutMs, Watcher watcher, RetryPolicy retryPolicy)
    {
        this(new DefaultZookeeperFactory(), ensembleProvider, sessionTimeoutMs, connectionTimeoutMs, watcher, retryPolicy, false, new StandardConnectionHandlingPolicy());
    }

    /**
     * @param zookeeperFactory factory for creating {@link ZooKeeper} instances
     * @param ensembleProvider the ensemble provider
     * @param sessionTimeoutMs session timeout
     * @param connectionTimeoutMs connection timeout
     * @param watcher default watcher or null
     * @param retryPolicy the retry policy to use
     * @param canBeReadOnly if true, allow ZooKeeper client to enter
     *                      read only mode in case of a network partition. See
     *                      {@link ZooKeeper#ZooKeeper(String, int, Watcher, long, byte[], boolean)}
     *                      for details
     */
    public CuratorZookeeperClient(ZookeeperFactory zookeeperFactory, EnsembleProvider ensembleProvider, int sessionTimeoutMs, int connectionTimeoutMs, Watcher watcher, RetryPolicy retryPolicy, boolean canBeReadOnly)
    {
        this(zookeeperFactory, ensembleProvider, sessionTimeoutMs, connectionTimeoutMs, watcher, retryPolicy, canBeReadOnly, new StandardConnectionHandlingPolicy());
    }

    /**
     * @param zookeeperFactory factory for creating {@link ZooKeeper} instances
     * @param ensembleProvider the ensemble provider
     * @param sessionTimeoutMs session timeout
     * @param connectionTimeoutMs connection timeout
     * @param watcher default watcher or null
     * @param retryPolicy the retry policy to use
     * @param canBeReadOnly if true, allow ZooKeeper client to enter
     *                      read only mode in case of a network partition. See
     *                      {@link ZooKeeper#ZooKeeper(String, int, Watcher, long, byte[], boolean)}
     *                      for details
     * @param connectionHandlingPolicy connection handling policy - use one of the pre-defined policies or write your own
     * @since 3.0.0
     */
    public CuratorZookeeperClient(ZookeeperFactory zookeeperFactory, EnsembleProvider ensembleProvider, int sessionTimeoutMs, int connectionTimeoutMs, Watcher watcher, RetryPolicy retryPolicy, boolean canBeReadOnly, ConnectionHandlingPolicy connectionHandlingPolicy) {
        this(zookeeperFactory, ensembleProvider, sessionTimeoutMs, connectionTimeoutMs, 0,
                watcher, retryPolicy, canBeReadOnly, connectionHandlingPolicy);
    }
    /**
     * @param zookeeperFactory factory for creating {@link ZooKeeper} instances
     * @param ensembleProvider the ensemble provider
     * @param sessionTimeoutMs session timeout
     * @param connectionTimeoutMs connection timeout
     * @param waitForShutdownTimeoutMs default timeout fo close operation
     * @param watcher default watcher or null
     * @param retryPolicy the retry policy to use
     * @param canBeReadOnly if true, allow ZooKeeper client to enter
     *                      read only mode in case of a network partition. See
     *                      {@link ZooKeeper#ZooKeeper(String, int, Watcher, long, byte[], boolean)}
     *                      for details
     * @param connectionHandlingPolicy connection handling policy - use one of the pre-defined policies or write your own
     * @since 4.0.2
     */
    public CuratorZookeeperClient(ZookeeperFactory zookeeperFactory, EnsembleProvider ensembleProvider,
            int sessionTimeoutMs, int connectionTimeoutMs, int waitForShutdownTimeoutMs, Watcher watcher,
            RetryPolicy retryPolicy, boolean canBeReadOnly, ConnectionHandlingPolicy connectionHandlingPolicy)
    {
        this.connectionHandlingPolicy = connectionHandlingPolicy;
        if ( sessionTimeoutMs < connectionTimeoutMs )
        {
            log.warn(String.format("session timeout [%d] is less than connection timeout [%d]", sessionTimeoutMs, connectionTimeoutMs));
        }

        retryPolicy = Preconditions.checkNotNull(retryPolicy, "retryPolicy cannot be null");
        ensembleProvider = Preconditions.checkNotNull(ensembleProvider, "ensembleProvider cannot be null");

        this.connectionTimeoutMs = connectionTimeoutMs;
        this.waitForShutdownTimeoutMs = waitForShutdownTimeoutMs;
        state = new ConnectionState(zookeeperFactory, ensembleProvider, sessionTimeoutMs, connectionTimeoutMs, watcher, tracer, canBeReadOnly, connectionHandlingPolicy);
        setRetryPolicy(retryPolicy);
    }

    /**
     * Return the managed ZK instance.
     *
     * @return client the client
     * @throws Exception if the connection timeout has elapsed or an exception occurs in a background process
     */
    public ZooKeeperAdmin getZooKeeper() throws Exception
    {
        Preconditions.checkState(started.get(), "Client is not started");

        return state.getZooKeeper();
    }

    /**
     * Return a new retry loop. All operations should be performed in a retry loop
     *
     * @return new retry loop
     */
    public RetryLoop newRetryLoop()
    {
        return new RetryLoopImpl(retryPolicy.get(), tracer);
    }

    /**
     * Return a new "session fail" retry loop. See {@link SessionFailRetryLoop} for details
     * on when to use it.
     *
     * @param mode failure mode
     * @return new retry loop
     */
    public SessionFailRetryLoop newSessionFailRetryLoop(SessionFailRetryLoop.Mode mode)
    {
        return new SessionFailRetryLoop(this, mode);
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
        Preconditions.checkState(started.get(), "Client is not started");

        log.debug("blockUntilConnectedOrTimedOut() start");
        OperationTrace       trace = startAdvancedTracer("blockUntilConnectedOrTimedOut");

        internalBlockUntilConnectedOrTimedOut();

        trace.commit();

        boolean localIsConnected = state.isConnected();
        log.debug("blockUntilConnectedOrTimedOut() end. isConnected: " + localIsConnected);

        return localIsConnected;
    }

    /**
     * Must be called after construction
     *
     * @throws IOException errors
     */
    public void start() throws Exception
    {
        log.debug("Starting");

        if ( !started.compareAndSet(false, true) )
        {
            throw new IllegalStateException("Already started");
        }

        state.start();
    }
    
    /**
     * Close the client.
     *
     * Same as {@link #close(int) } using the timeout set at construction time.
     *
     * @see #close(int)
     */
    @Override
    public void close() {
        close(waitForShutdownTimeoutMs);
    }
            
    /**
     * Close this client object as the {@link #close() } method.
     * This method will wait for internal resources to be released.
     * 
     * @param waitForShutdownTimeoutMs timeout (in milliseconds) to wait for resources to be released.
     *                  Use zero or a negative value to skip the wait.
     */
    public void close(int waitForShutdownTimeoutMs)
    {
        log.debug("Closing, waitForShutdownTimeoutMs {}", waitForShutdownTimeoutMs);

        started.set(false);
        try
        {
            state.close(waitForShutdownTimeoutMs);
        }
        catch ( IOException e )
        {
            ThreadUtils.checkInterrupted(e);
            log.error("", e);
        }
    }

    /**
     * Change the retry policy
     *
     * @param policy new policy
     */
    public void setRetryPolicy(RetryPolicy policy)
    {
        Preconditions.checkNotNull(policy, "policy cannot be null");

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
     * Start a new tracer
     * @param name name of the event
     * @return the new tracer ({@link TimeTrace#commit()} must be called)
     */
    public TimeTrace startTracer(String name)
    {
        return new TimeTrace(name, tracer.get());
    }

    /**
     * Start a new advanced tracer with more metrics being recorded
     * @param name name of the event
     * @return the new tracer ({@link OperationTrace#commit()} must be called)
     */
    public OperationTrace          startAdvancedTracer(String name)
    {
        return new OperationTrace(name, tracer.get(), state.getSessionId());
    }

    /**
     * Return the current tracing driver
     *
     * @return tracing driver
     */
    public TracerDriver getTracerDriver()
    {
        return tracer.get();
    }

    /**
     * Change the tracing driver
     *
     * @param tracer new tracing driver
     */
    public void setTracerDriver(TracerDriver tracer)
    {
        this.tracer.set(tracer);
    }

    /**
     * Returns the current known connection string - not guaranteed to be correct
     * value at any point in the future.
     *
     * @return connection string
     */
    public String getCurrentConnectionString()
    {
        return state.getEnsembleProvider().getConnectionString();
    }

    /**
     * Return the configured connection timeout
     *
     * @return timeout
     */
    public int getConnectionTimeoutMs()
    {
        return connectionTimeoutMs;
    }

    /**
     * For internal use only - reset the internally managed ZK handle
     *
     * @throws Exception errors
     */
    public void reset() throws Exception
    {
        state.reset();
    }

    /**
     * Every time a new {@link ZooKeeper} instance is allocated, the "instance index"
     * is incremented.
     *
     * @return the current instance index
     */
    public long getInstanceIndex()
    {
        return state.getInstanceIndex();
    }

    /**
     * Return the configured connection handling policy
     *
     * @return ConnectionHandlingPolicy
     */
    public ConnectionHandlingPolicy getConnectionHandlingPolicy()
    {
        return connectionHandlingPolicy;
    }

    /**
     * Return the most recent value of {@link ZooKeeper#getSessionTimeout()} or 0
     *
     * @return session timeout or 0
     */
    public int getLastNegotiatedSessionTimeoutMs()
    {
        return state.getLastNegotiatedSessionTimeoutMs();
    }

    void addParentWatcher(Watcher watcher)
    {
        state.addParentWatcher(watcher);
    }

    void removeParentWatcher(Watcher watcher)
    {
        state.removeParentWatcher(watcher);
    }

    /**
     * For internal use only
     *
     * @throws InterruptedException interruptions
     */
    public void internalBlockUntilConnectedOrTimedOut() throws InterruptedException
    {
        long waitTimeMs = connectionTimeoutMs;
        while ( !state.isConnected() && (waitTimeMs > 0) )
        {
            final CountDownLatch latch = new CountDownLatch(1);
            Watcher tempWatcher = new Watcher()
            {
                @Override
                public void process(WatchedEvent event)
                {
                    latch.countDown();
                }
            };

            state.addParentWatcher(tempWatcher);
            long startTimeMs = System.currentTimeMillis();
            long timeoutMs = Math.min(waitTimeMs, 1000);
            try
            {
                latch.await(timeoutMs, TimeUnit.MILLISECONDS);
            }
            finally
            {
                state.removeParentWatcher(tempWatcher);
            }
            long elapsed = Math.max(1, System.currentTimeMillis() - startTimeMs);
            waitTimeMs -= elapsed;
        }
    }
}
