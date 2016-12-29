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
package org.apache.curator.framework.recipes.watch;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.listen.ListenerContainer;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import java.io.Closeable;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public class PersistentWatcher implements Closeable
{
    private final AtomicReference<State> state = new AtomicReference<>(State.LATENT);
    private final ListenerContainer<Watcher> listeners = new ListenerContainer<>();
    private final ConnectionStateListener connectionStateListener = new ConnectionStateListener()
    {
        @Override
        public void stateChanged(CuratorFramework client, ConnectionState newState)
        {
            if ( newState.isConnected() )
            {
                reset();
            }
        }
    };
    private final Watcher watcher = new Watcher()
    {
        @Override
        public void process(final WatchedEvent event)
        {
            Function<Watcher, Void> function = new Function<Watcher, Void>()
            {
                @Override
                public Void apply(Watcher watcher)
                {
                    watcher.process(event);
                    return null;
                }
            };
            listeners.forEach(function);
        }
    };
    private final CuratorFramework client;
    private final String basePath;

    private enum State
    {
        LATENT,
        STARTED,
        CLOSED
    }

    public PersistentWatcher(CuratorFramework client, String basePath)
    {
        this.client = Objects.requireNonNull(client, "client cannot be null");
        this.basePath = Objects.requireNonNull(basePath, "basePath cannot be null");
    }

    public void start()
    {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "Already started");
        client.getConnectionStateListenable().addListener(connectionStateListener);
        reset();
    }

    @Override
    public void close()
    {
        if ( state.compareAndSet(State.STARTED, State.CLOSED) )
        {
            client.getConnectionStateListenable().removeListener(connectionStateListener);
            try
            {
                client.watches().remove(watcher).inBackground().forPath(basePath);
            }
            catch ( Exception e )
            {
                // TODO
            }
        }
    }

    public Listenable<Watcher> getListenable()
    {
        return listeners;
    }

    private void reset()
    {
        try
        {
            BackgroundCallback callback = new BackgroundCallback()
            {
                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
                {
                    if ( event.getResultCode() == 0 )
                    {
                        watcherSet();
                    }
                }
            };
            client.addPersistentWatch().inBackground().usingWatcher(watcher).forPath(basePath);
        }
        catch ( Exception e )
        {
            // TODO
        }
    }

    protected void watcherSet()
    {
        // default is NOP
    }
}
