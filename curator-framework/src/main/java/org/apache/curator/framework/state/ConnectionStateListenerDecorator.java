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

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import java.util.concurrent.ScheduledExecutorService;

/**
 * <p>
 *     Allows for the enhancement of the {@link org.apache.curator.framework.state.ConnectionStateListener} instances
 *     used with Curator. Client code that sets a ConnectionStateListener should always wrap it using the configured
 *     ConnectionStateListenerDecorator. All Curator recipes do this.
 * </p>
 *
 * <p>
 *     E.g.
 *
 * <code><pre>
 * CuratorFramework client ...
 * ConnectionStateListener listener = ...
 * ConnectionStateListener decoratedListener = client.decorateConnectionStateListener(listener);
 *
 * ...
 *
 * client.getConnectionStateListenable().addListener(decoratedListener);
 *
 * // later, to remove...
 * client.getConnectionStateListenable().removeListener(decoratedListener);
 * </pre></code>
 * </p>
 */
@FunctionalInterface
public interface ConnectionStateListenerDecorator
{
    ConnectionStateListener decorateListener(CuratorFramework client, ConnectionStateListener actual);

    /**
     * Pass through - does no decoration
     */
    ConnectionStateListenerDecorator standard = (__, actual) -> actual;

    /**
     * Decorates the listener with circuit breaking behavior using {@link org.apache.curator.framework.state.CircuitBreakingConnectionStateListener}
     *
     * @param retryPolicy the circuit breaking policy to use
     * @return new decorator
     */
    static ConnectionStateListenerDecorator circuitBreaking(RetryPolicy retryPolicy)
    {
        return (client, actual) -> new CircuitBreakingConnectionStateListener(client, actual, retryPolicy);
    }

    /**
     * Decorates the listener with circuit breaking behavior using {@link org.apache.curator.framework.state.CircuitBreakingConnectionStateListener}
     *
     * @param retryPolicy the circuit breaking policy to use
     * @param service the scheduler to use
     * @return new decorator
     */
    static ConnectionStateListenerDecorator circuitBreaking(RetryPolicy retryPolicy, ScheduledExecutorService service)
    {
        return (client, actual) -> new CircuitBreakingConnectionStateListener(client, actual, retryPolicy, service);
    }
}
