/*
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

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;

/**
 * <p>
 *     A proxy for connection state listeners that adds circuit breaking behavior. During network
 *     outages ZooKeeper can become very noisy sending connection/disconnection events in rapid succession.
 *     Curator recipes respond to these messages by resetting state, etc. E.g. LeaderLatch must delete
 *     its lock node and try to recreate it in order to try to re-obtain leadership, etc.
 * </p>
 *
 * <p>
 *     This noisy herding can be avoided by using the circuit breaking listener. When it
 *     receives {@link org.apache.curator.framework.state.ConnectionState#SUSPENDED}, the circuit
 *     becomes "open" (based on the provided {@link org.apache.curator.RetryPolicy}) and will ignore
 *     future connection state changes until RetryPolicy timeout has elapsed. Note: however, if the connection
 *     goes from {@link org.apache.curator.framework.state.ConnectionState#SUSPENDED} to
 *     {@link org.apache.curator.framework.state.ConnectionState#LOST} the first LOST state <i>is</i> sent.
 * </p>
 *
 * <p>
 *     When the circuit is closed, all connection state changes are forwarded to the managed
 *     listener. When the first disconnected state is received, the circuit becomes open. The state change
 *     that caused the circuit to open is sent to the managed listener and the RetryPolicy will be used to
 *     get a delay amount. While the delay is active, the circuit breaker will store state changes but will not
 *     forward them to the managed listener (except, however, the first time the state changes from SUSPENDED to LOST).
 *     When the delay elapses, if the connection has been restored, the circuit closes and forwards the
 *     new state to the managed listener. If the connection has not been restored, the RetryPolicy is checked
 *     again. If the RetryPolicy indicates another retry is allowed the process repeats. If, however, the
 *     RetryPolicy indicates that retries are exhausted then the circuit closes - if the current state
 *     is different than the state that caused the circuit to open it is forwarded to the managed listener.
 * </p>
 *
 * <p>
 *     <strong>NOTE:</strong> You should not use this listener directly. Instead, set {@link org.apache.curator.framework.state.ConnectionStateListenerManagerFactory#circuitBreaking(org.apache.curator.RetryPolicy)}
 *     in the {@link org.apache.curator.framework.CuratorFrameworkFactory.Builder#connectionStateListenerManagerFactory(ConnectionStateListenerManagerFactory)}.
 * </p>
 *
 * <p>
 *     E.g.
 * <code><pre>
 * ConnectionStateListenerManagerFactory factory = ConnectionStateListenerManagerFactory.circuitBreaking(...retry policy for circuit breaking...);
 * CuratorFramework client = CuratorFrameworkFactory.builder()
 *     .connectionStateListenerManagerFactory(factory)
 *     ... etc ...
 *     .build();
 * // all connection state listeners set for "client" will get circuit breaking behavior
 * </pre></code>
 * </p>
 */
public class CircuitBreakingConnectionStateListener implements ConnectionStateListener
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final CuratorFramework client;
    private final ConnectionStateListener listener;
    private final CircuitBreaker circuitBreaker;

    // guarded by sync
    private boolean circuitLostHasBeenSent;
    // guarded by sync
    private ConnectionState circuitLastState;
    // guarded by sync
    private ConnectionState circuitInitialState;

    /**
     * @param client Curator instance
     * @param listener listener to manage
     * @param retryPolicy breaking policy to use
     */
    public CircuitBreakingConnectionStateListener(CuratorFramework client, ConnectionStateListener listener, RetryPolicy retryPolicy)
    {
        this(client, listener, CircuitBreaker.build(retryPolicy));
    }

    /**
     * @param client Curator instance
     * @param listener listener to manage
     * @param retryPolicy breaking policy to use
     * @param service scheduler to use
     */
    public CircuitBreakingConnectionStateListener(CuratorFramework client, ConnectionStateListener listener, RetryPolicy retryPolicy, ScheduledExecutorService service)
    {
        this(client, listener, CircuitBreaker.build(retryPolicy, service));
    }

    CircuitBreakingConnectionStateListener(CuratorFramework client, ConnectionStateListener listener, CircuitBreaker circuitBreaker)
    {
        this.client = Objects.requireNonNull(client, "client cannot be null");
        this.listener = Objects.requireNonNull(listener, "listener cannot be null");
        this.circuitBreaker = Objects.requireNonNull(circuitBreaker, "circuitBreaker cannot be null");
        reset();
    }

    @Override
    public synchronized void stateChanged(CuratorFramework client, ConnectionState newState)
    {
        if ( circuitBreaker.isOpen() )
        {
            handleOpenStateChange(newState);
        }
        else
        {
            handleClosedStateChange(newState);
        }
    }

    /**
     * Returns true if the circuit is open
     *
     * @return true/false
     */
    public synchronized boolean isOpen()
    {
        return circuitBreaker.isOpen();
    }

    private synchronized void handleClosedStateChange(ConnectionState newState)
    {
        if ( !newState.isConnected() )
        {
            if ( circuitBreaker.tryToOpen(this::checkCloseCircuit) )
            {
                log.info("Circuit is opening. State: {} post-retryCount: {}", newState, circuitBreaker.getRetryCount());
                circuitLastState = circuitInitialState = newState;
                circuitLostHasBeenSent = (newState == ConnectionState.LOST);
            }
            else
            {
                log.debug("Could not open circuit breaker. State: {}", newState);
            }
        }
        callListener(newState);
    }

    private synchronized void handleOpenStateChange(ConnectionState newState)
    {
        if ( circuitLostHasBeenSent || (newState != ConnectionState.LOST) )
        {
            log.debug("Circuit is open. Ignoring state change: {}", newState);
            circuitLastState = newState;
        }
        else
        {
            log.debug("Circuit is open. State changed to LOST. Sending to listener.");
            circuitLostHasBeenSent = true;
            circuitLastState = circuitInitialState = ConnectionState.LOST;
            callListener(ConnectionState.LOST);
        }
    }

    private synchronized void checkCloseCircuit()
    {
        if ( (circuitLastState == null) || circuitLastState.isConnected() )
        {
            log.info("Circuit is closing. Initial state: {} - Last state: {}", circuitInitialState, circuitLastState);
            closeCircuit();
        }
        else if ( circuitBreaker.tryToRetry(this::checkCloseCircuit) )
        {
            log.debug("Circuit open is continuing due to retry. State: {} post-retryCount: {}", circuitLastState, circuitBreaker.getRetryCount());
        }
        else
        {
            log.info("Circuit is closing due to retries exhausted. Initial state: {} - Last state: {}", circuitInitialState, circuitLastState);
            closeCircuit();
        }
    }

    private synchronized void callListener(ConnectionState newState)
    {
        if ( newState != null )
        {
            listener.stateChanged(client, newState);
        }
    }

    private synchronized void closeCircuit()
    {
        ConnectionState stateToSend = (circuitLastState == circuitInitialState) ? null : circuitLastState;
        reset();
        callListener(stateToSend);
    }

    private synchronized void reset()
    {
        circuitLastState = null;
        circuitInitialState = null;
        circuitLostHasBeenSent = false;
        circuitBreaker.close();
    }
}
