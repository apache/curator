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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@VisibleForTesting
public class EnsembleTracker implements Closeable, CuratorWatcher
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final WatcherRemoveCuratorFramework client;
    private final EnsembleProvider ensembleProvider;
    private final AtomicReference<State> state = new AtomicReference<>(State.LATENT);
    private final AtomicInteger outstanding = new AtomicInteger(0);
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

    @VisibleForTesting
    public boolean hasOutstanding()
    {
        return outstanding.get() > 0;
    }

    private void reset() throws Exception
    {
        if ( (client.getState() == CuratorFrameworkState.STARTED) && (state.get() == State.STARTED) )
        {
            BackgroundCallback backgroundCallback = new BackgroundCallback()
            {
                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
                {
                    outstanding.decrementAndGet();
                    if ( (event.getType() == CuratorEventType.GET_CONFIG) && (event.getResultCode() == KeeperException.Code.OK.intValue()) )
                    {
                        processConfigData(event.getData());
                    }
                }
            };
            outstanding.incrementAndGet();
            try
            {
                client.getConfig().usingWatcher(this).inBackground(backgroundCallback).forEnsemble();
                outstanding.incrementAndGet();  // finally block will decrement
            }
            finally
            {
                outstanding.decrementAndGet();
            }
        }
    }

    @VisibleForTesting
    public static String configToConnectionString(QuorumVerifier data) throws Exception
    {
        StringBuilder sb = new StringBuilder();
        for ( QuorumPeer.QuorumServer server : data.getAllMembers().values() )
        {
            if ( server.clientAddr == null )
            {
                // Invalid client address configuration in zoo.cfg
                continue;
            }
            if ( sb.length() != 0 )
            {
                sb.append(",");
            }
            InetAddress wildcardAddress = new InetSocketAddress(0).getAddress();
            String hostAddress;
            if ( wildcardAddress.equals(server.clientAddr.getAddress()) )
            {
                hostAddress = server.addr.getAddress().getHostAddress();
            }
            else
            {
                hostAddress = server.clientAddr.getAddress().getHostAddress();
            }
            sb.append(hostAddress).append(":").append(server.clientAddr.getPort());
        }

        return sb.toString();
    }

    private void processConfigData(byte[] data) throws Exception
    {
        Properties properties = new Properties();
        properties.load(new ByteArrayInputStream(data));
        log.info("New config event received: {}", properties);

        if (!properties.isEmpty())
        {
            QuorumMaj newConfig = new QuorumMaj(properties);
            String connectionString = configToConnectionString(newConfig);
            if (connectionString.trim().length() > 0)
            {
                currentConfig.set(newConfig);
                ensembleProvider.setConnectionString(connectionString);
            }
            else
            {
                log.error("Invalid config event received: {}", properties);
            }
        }
        else
        {
            log.debug("Ignoring new config as it is empty");
        }
    }
}
