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

package org.apache.curator.framework.ensemble;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import org.apache.curator.ensemble.EnsembleListener;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.listen.ListenerContainer;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.flexible.QuorumMaj;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tracks changes to the ensemble and notifies registered {@link org.apache.curator.ensemble.EnsembleListener} instances.
 */
public class EnsembleTracker implements Closeable
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final CuratorFramework client;
    private final AtomicReference<State> state = new AtomicReference<>(State.LATENT);
    private final ListenerContainer<EnsembleListener> listeners = new ListenerContainer<>();
    private final ConnectionStateListener connectionStateListener = new ConnectionStateListener()
    {
        @Override
        public void stateChanged(CuratorFramework client, ConnectionState newState)
        {
            if ( (newState == ConnectionState.CONNECTED) || (newState == ConnectionState.RECONNECTED) )
            {
                try
                {
                    reset();
                }
                catch ( Exception e )
                {
                    log.error("Trying to reset after reconnection", e);
                }
            }
        }
    };

    private final CuratorWatcher watcher = new CuratorWatcher()
    {
        @Override
        public void process(WatchedEvent event) throws Exception
        {
            if ( event.getType() == Watcher.Event.EventType.NodeDataChanged )
            {
                reset();
            }
        }
    };

    private enum State
    {
        LATENT,
        STARTED,
        CLOSED
    }

    private final BackgroundCallback backgroundCallback = new BackgroundCallback()
    {
        @Override
        public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
        {
            processBackgroundResult(event);
        }
    };

    public EnsembleTracker(CuratorFramework client)
    {
        this.client = client;
    }

    public void start() throws Exception
    {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "Cannot be started more than once");
        client.getConnectionStateListenable().addListener(connectionStateListener);
        reset();
    }

    @Override
    public void close() throws IOException
    {
        if ( state.compareAndSet(State.STARTED, State.CLOSED) )
        {
            listeners.clear();
        }
        client.getConnectionStateListenable().removeListener(connectionStateListener);
    }

    /**
     * Return the ensemble listenable
     *
     * @return listenable
     */
    public ListenerContainer<EnsembleListener> getListenable()
    {
        Preconditions.checkState(state.get() != State.CLOSED, "Closed");

        return listeners;
    }

    private void reset() throws Exception
    {
        client.getConfig().usingWatcher(watcher).inBackground(backgroundCallback).forEnsemble();
    }

    private void processBackgroundResult(CuratorEvent event) throws Exception
    {
        switch ( event.getType() )
        {
            case GET_CONFIG:
            {
                if ( event.getResultCode() == KeeperException.Code.OK.intValue() )
                {
                    processConfigData(event.getData());
                }
            }
        }
    }

    private void processConfigData(byte[] data) throws Exception
    {
        Properties properties = new Properties();
        properties.load(new ByteArrayInputStream(data));
        QuorumVerifier qv = new QuorumMaj(properties);
        StringBuilder sb = new StringBuilder();
        for ( QuorumPeer.QuorumServer server : qv.getAllMembers().values() )
        {
            if ( sb.length() != 0 )
            {
                sb.append(",");
            }
            sb.append(server.clientAddr.getAddress().getHostAddress()).append(":").append(server.clientAddr.getPort());
        }

        final String connectionString = sb.toString();
        listeners.forEach
            (
                new Function<EnsembleListener, Void>()
                {
                    @Override
                    public Void apply(EnsembleListener listener)
                    {
                        try
                        {
                            listener.connectionStringUpdated(connectionString);
                        }
                        catch ( Exception e )
                        {
                            log.error("Calling listener", e);
                        }
                        return null;
                    }
                }
            );
    }
}
