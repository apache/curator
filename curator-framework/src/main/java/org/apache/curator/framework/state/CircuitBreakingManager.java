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

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.listen.StandardListenerManager;
import org.apache.curator.framework.listen.UnaryListenerManager;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

class CircuitBreakingManager implements UnaryListenerManager<ConnectionStateListener>
{
    private final StandardListenerManager<ConnectionStateListener> mainContainer = StandardListenerManager.standard();
    private final StandardListenerManager<ConnectionStateListener> doNotProxyContainer = StandardListenerManager.standard();
    private final CircuitBreakingConnectionStateListener masterListener;

    CircuitBreakingManager(CuratorFramework client, CircuitBreaker circuitBreaker)
    {
        ConnectionStateListener masterStateChanged = (__, newState) -> mainContainer.forEach(listener -> listener.stateChanged(client, newState));
        masterListener = new CircuitBreakingConnectionStateListener(client, masterStateChanged, circuitBreaker);
    }

    @Override
    public void clear()
    {
        doNotProxyContainer.clear();
        mainContainer.clear();
    }

    @Override
    public int size()
    {
        return mainContainer.size() + doNotProxyContainer.size();
    }

    @Override
    public void forEach(Consumer<ConnectionStateListener> function)
    {
        doNotProxyContainer.forEach(function);
        function.accept(masterListener);
    }

    @Override
    public void addListener(ConnectionStateListener listener)
    {
        if ( listener.doNotProxy() )
        {
            doNotProxyContainer.addListener(listener);
        }
        else
        {
            mainContainer.addListener(listener);
        }
    }

    @Override
    public void addListener(ConnectionStateListener listener, Executor executor)
    {
        if ( listener.doNotProxy() )
        {
            doNotProxyContainer.addListener(listener, executor);
        }
        else
        {
            mainContainer.addListener(listener, executor);
        }
    }

    @Override
    public void removeListener(ConnectionStateListener listener)
    {
        mainContainer.removeListener(listener);
        doNotProxyContainer.removeListener(listener);
    }
}
