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

package org.apache.curator.framework.state;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.listen.ListenerContainer;
import org.apache.curator.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Used internally to manage connection state
 */
public class ConnectionStateManager implements Closeable
{
    private static final int QUEUE_SIZE;

    static
    {
        int size = 25;
        String property = System.getProperty("ConnectionStateManagerSize", null);
        if ( property != null )
        {
            try
            {
                size = Integer.parseInt(property);
            }
            catch ( NumberFormatException ignore )
            {
                // ignore
            }
        }
        QUEUE_SIZE = size;
    }

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final BlockingQueue<ConnectionState> eventQueue = new ArrayBlockingQueue<ConnectionState>(QUEUE_SIZE);
    private final CuratorFramework client;
    private final boolean enableSessionExpiredState;
    private final int sessionTimeoutMs;
    private final ListenerContainer<ConnectionStateListener> listeners = new ListenerContainer<ConnectionStateListener>();
    private final AtomicBoolean initialConnectMessageSent = new AtomicBoolean(false);
    private final ExecutorService service;
    private final AtomicReference<State> state = new AtomicReference<State>(State.LATENT);

    // guarded by sync
    private ConnectionState currentConnectionState;
    // guarded by sync
    private long startOfSuspendedEpoch = 0;

    private enum State
    {
        LATENT,
        STARTED,
        CLOSED
    }

    /**
     * @param client        the client
     * @param threadFactory thread factory to use or null for a default
     * @param enableSessionExpiredState if true, applies new meaning for LOST as described here: {@link ConnectionState#LOST}
     * @param sessionTimeoutMs the ZK session timeout in milliseconds
     */
    public ConnectionStateManager(CuratorFramework client, ThreadFactory threadFactory, boolean enableSessionExpiredState, int sessionTimeoutMs)
    {
        this.client = client;
        this.enableSessionExpiredState = enableSessionExpiredState;
        this.sessionTimeoutMs = sessionTimeoutMs;
        if ( threadFactory == null )
        {
            threadFactory = ThreadUtils.newThreadFactory("ConnectionStateManager");
        }
        service = Executors.newSingleThreadExecutor(threadFactory);
    }

    /**
     * Start the manager
     */
    public void start()
    {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "Cannot be started more than once");

        service.submit
            (
                new Callable<Object>()
                {
                    @Override
                    public Object call() throws Exception
                    {
                        processEvents();
                        return null;
                    }
                }
            );
    }

    @Override
    public void close()
    {
        if ( state.compareAndSet(State.STARTED, State.CLOSED) )
        {
            service.shutdownNow();
            listeners.clear();
        }
    }

    /**
     * Return the listenable
     *
     * @return listenable
     */
    public ListenerContainer<ConnectionStateListener> getListenable()
    {
        return listeners;
    }

    /**
     * Change to {@link ConnectionState#SUSPENDED} only if not already suspended and not lost
     *
     * @return true if connection is set to SUSPENDED
     */
    public synchronized boolean setToSuspended()
    {
        if ( state.get() != State.STARTED )
        {
            return false;
        }

        if ( (currentConnectionState == ConnectionState.LOST) || (currentConnectionState == ConnectionState.SUSPENDED) )
        {
            return false;
        }

        setCurrentConnectionState(ConnectionState.SUSPENDED);
        postState(ConnectionState.SUSPENDED);

        return true;
    }

    /**
     * Post a state change. If the manager is already in that state the change
     * is ignored. Otherwise the change is queued for listeners.
     *
     * @param newConnectionState new state
     * @return true if the state actually changed, false if it was already at that state
     */
    public synchronized boolean addStateChange(ConnectionState newConnectionState)
    {
        if ( state.get() != State.STARTED )
        {
            return false;
        }

        ConnectionState previousState = currentConnectionState;
        if ( previousState == newConnectionState )
        {
            return false;
        }
        setCurrentConnectionState(newConnectionState);

        ConnectionState localState = newConnectionState;
        boolean isNegativeMessage = ((newConnectionState == ConnectionState.LOST) || (newConnectionState == ConnectionState.SUSPENDED) || (newConnectionState == ConnectionState.READ_ONLY));
        if ( !isNegativeMessage && initialConnectMessageSent.compareAndSet(false, true) )
        {
            localState = ConnectionState.CONNECTED;
        }

        postState(localState);

        return true;
    }

    public synchronized boolean blockUntilConnected(int maxWaitTime, TimeUnit units) throws InterruptedException
    {
        long startTime = System.currentTimeMillis();

        boolean hasMaxWait = (units != null);
        long maxWaitTimeMs = hasMaxWait ? TimeUnit.MILLISECONDS.convert(maxWaitTime, units) : 0;

        while ( !isConnected() )
        {
            if ( hasMaxWait )
            {
                long waitTime = maxWaitTimeMs - (System.currentTimeMillis() - startTime);
                if ( waitTime <= 0 )
                {
                    return isConnected();
                }

                wait(waitTime);
            }
            else
            {
                wait();
            }
        }
        return isConnected();
    }

    public synchronized boolean isConnected()
    {
        return (currentConnectionState != null) && currentConnectionState.isConnected();
    }

    private void postState(ConnectionState state)
    {
        log.info("State change: " + state);

        notifyAll();

        while ( !eventQueue.offer(state) )
        {
            eventQueue.poll();
            log.warn("ConnectionStateManager queue full - dropping events to make room");
        }
    }

    private void processEvents()
    {
        try
        {
            while ( !Thread.currentThread().isInterrupted() )
            {
                int pollMaxMs = (sessionTimeoutMs * 2) / 3; // 2/3 of session timeout
                final ConnectionState newState = eventQueue.poll(pollMaxMs, TimeUnit.MILLISECONDS);
                if ( newState != null )
                {
                    if ( listeners.size() == 0 )
                    {
                        log.warn("There are no ConnectionStateListeners registered.");
                    }

                    listeners.forEach
                        (
                            new Function<ConnectionStateListener, Void>()
                            {
                                @Override
                                public Void apply(ConnectionStateListener listener)
                                {
                                    listener.stateChanged(client, newState);
                                    return null;
                                }
                            }
                        );
                }
                else if ( enableSessionExpiredState )
                {
                    synchronized(this)
                    {
                        checkSessionExpiration();
                    }
                }
            }
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
        }
    }

    private void checkSessionExpiration()
    {
        if ( (currentConnectionState == ConnectionState.SUSPENDED) && (startOfSuspendedEpoch != 0) )
        {
            long elapsedMs = System.currentTimeMillis() - startOfSuspendedEpoch;
            if ( elapsedMs >= sessionTimeoutMs )
            {
                log.warn(String.format("Session timeout has elapsed while SUSPENDED. Posting LOST event and resetting the connection. Elapsed ms: %d", elapsedMs));
                try
                {
                    client.getZookeeperClient().reset();
                }
                catch ( Exception e )
                {
                    log.error("Could not reset the connection", e);
                }
                addStateChange(ConnectionState.LOST);
            }
        }
    }

    private void setCurrentConnectionState(ConnectionState newConnectionState)
    {
        currentConnectionState = newConnectionState;
        startOfSuspendedEpoch = (currentConnectionState == ConnectionState.SUSPENDED) ? System.currentTimeMillis() : 0;
    }
}
