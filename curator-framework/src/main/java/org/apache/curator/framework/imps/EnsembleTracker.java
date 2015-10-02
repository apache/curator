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

package org.apache.curator.framework.imps;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.curator.ensemble.EnsembleProvider;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.WatcherRemoveCuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.flexible.QuorumMaj;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

@VisibleForTesting
public class EnsembleTracker implements Closeable, CuratorWatcher
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final WatcherRemoveCuratorFramework client;
    private final EnsembleProvider ensembleProvider;
    private final AtomicReference<State> state = new AtomicReference<>(State.LATENT);
    private final AtomicReference<QuorumMaj> currentConfig = new AtomicReference<>(new QuorumMaj(Maps.<Long, QuorumPeer.QuorumServer>newHashMap()));
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

    private enum State
    {
        LATENT,
        STARTED,
        CLOSED
    }

    EnsembleTracker(CuratorFramework client, EnsembleProvider ensembleProvider)
    {
        this.client = client.newWatcherRemoveCuratorFramework();
        this.ensembleProvider = ensembleProvider;
    }

    public void start() throws Exception
    {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "Cannot be started more than once");
        client.getConnectionStateListenable().addListener(connectionStateListener);
        reset();
    }

    @Override
    public void close()
    {
        if ( state.compareAndSet(State.STARTED, State.CLOSED) )
        {
            client.removeWatchers();
            client.getConnectionStateListenable().removeListener(connectionStateListener);
        }
    }

    @Override
    public void process(WatchedEvent event) throws Exception
    {
        if ( event.getType() == Watcher.Event.EventType.NodeDataChanged )
        {
            reset();
        }
    }

    /**
     * Return the current quorum config
     *
     * @return config
     */
    public QuorumVerifier getCurrentConfig()
    {
        return currentConfig.get();
    }

    private void reset() throws Exception
    {
        BackgroundCallback backgroundCallback = new BackgroundCallback()
        {
            @Override
            public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
            {
                if ( event.getType() == CuratorEventType.GET_CONFIG )
                {
                    processConfigData(event.getData());
                }
            }
        };
        client.getConfig().usingWatcher(this).inBackground(backgroundCallback).forEnsemble();
    }

    @VisibleForTesting
    public static String configToConnectionString(QuorumVerifier data) throws Exception
    {
        StringBuilder sb = new StringBuilder();
        for ( QuorumPeer.QuorumServer server : data.getAllMembers().values() )
        {
            if ( sb.length() != 0 )
            {
                sb.append(",");
            }
            sb.append(server.clientAddr.getAddress().getHostAddress()).append(":").append(server.clientAddr.getPort());
        }

        return sb.toString();
    }

    private void processConfigData(byte[] data) throws Exception
    {
        log.info("New config event received: " + Arrays.toString(data));

        Properties properties = new Properties();
        properties.load(new ByteArrayInputStream(data));
        QuorumMaj newConfig = new QuorumMaj(properties);
        currentConfig.set(newConfig);

        String connectionString = configToConnectionString(newConfig);
        if ( connectionString.trim().length() > 0 )
        {
            ensembleProvider.setConnectionString(connectionString);
        }
        else
        {
            log.debug("Ignoring new config as it is empty");
        }
    }
}
