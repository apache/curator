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
import org.apache.curator.framework.listen.StandardListenerManager;
import org.apache.curator.framework.listen.UnaryListenerManager;
import java.util.concurrent.ScheduledExecutorService;

@FunctionalInterface
public interface ConnectionStateListenerManagerFactory
{
    /**
     * Create a new listener manager
     *
     * @param client curator client
     * @return manager
     */
    UnaryListenerManager<ConnectionStateListener> newManager(CuratorFramework client);

    /**
     * Pass through
     */
    ConnectionStateListenerManagerFactory standard = (__) -> StandardListenerManager.standard();

    /**
     * Listeners set in this manager receive circuit breaking behavior using {@link org.apache.curator.framework.state.CircuitBreakingConnectionStateListener}
     * as a master listener that proxies to any listener registered by client code (unless the listener returns true
     * for {@link ConnectionStateListener#doNotProxy()}).
     *
     * @param retryPolicy the circuit breaking policy to use
     * @return new listener manager factory
     */
    static ConnectionStateListenerManagerFactory circuitBreaking(RetryPolicy retryPolicy)
    {
        return client -> new CircuitBreakingManager(client, CircuitBreaker.build(retryPolicy));
    }

    /**
     * Listeners set in this manager receive circuit breaking behavior using {@link org.apache.curator.framework.state.CircuitBreakingConnectionStateListener}
     * as a master listener that proxies to any listener registered by client code (unless the listener returns true
     * for {@link ConnectionStateListener#doNotProxy()}).
     *
     * @param retryPolicy the circuit breaking policy to use
     * @param service the scheduler to use
     * @return new listener manager factory
     */
    static ConnectionStateListenerManagerFactory circuitBreaking(RetryPolicy retryPolicy, ScheduledExecutorService service)
    {
        return client -> new CircuitBreakingManager(client, CircuitBreaker.build(retryPolicy, service));
    }
}
