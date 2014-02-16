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
package org.apache.curator.x.rest;

import com.google.common.base.Preconditions;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.utils.ThreadUtils;
import org.apache.curator.x.rest.api.Session;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class CuratorRestContext implements Closeable
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Session session = new Session();
    private final ObjectMapper mapper = new ObjectMapper();
    private final ObjectWriter writer = mapper.writer();
    private final CuratorFramework client;
    private final int sessionLengthMs;
    private final AtomicReference<State> state = new AtomicReference<State>(State.LATENT);
    private final ScheduledExecutorService executorService = ThreadUtils.newSingleThreadScheduledExecutor("CuratorRestContext");
    private final ConnectionStateListener connectionStateListener = new ConnectionStateListener()
    {
        @Override
        public void stateChanged(CuratorFramework client, ConnectionState newState)
        {
            if ( newState == ConnectionState.LOST )
            {
                handleLostConnection();
            }
        }
    };

    private enum State
    {
        LATENT,
        STARTED,
        CLOSED
    }

    public CuratorRestContext(CuratorFramework client, int sessionLengthMs)
    {
        this.client = client;
        this.sessionLengthMs = sessionLengthMs;
    }

    public CuratorFramework getClient()
    {
        Preconditions.checkState(state.get() == State.STARTED, "Not started");
        return client;
    }

    public Session getSession()
    {
        Preconditions.checkState(state.get() == State.STARTED, "Not started");
        session.updateLastUse();
        return session;
    }

    public void start()
    {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "Already started");

        client.getConnectionStateListenable().addListener(connectionStateListener);

        Runnable runner = new Runnable()
        {
            @Override
            public void run()
            {
                checkSession();
            }
        };
        executorService.scheduleAtFixedRate(runner, sessionLengthMs, sessionLengthMs, TimeUnit.MILLISECONDS);
    }

    private void checkSession()
    {
        long elapsedSinceLastUse = System.currentTimeMillis() - session.getLastUseMs();
        if ( elapsedSinceLastUse > sessionLengthMs )
        {
            log.warn("Session has expired. Closing all open recipes. Milliseconds since last ping: " + elapsedSinceLastUse);
            session.closeThings();
        }
    }

    @Override
    public void close()
    {
        if ( state.compareAndSet(State.STARTED, State.CLOSED) )
        {
            client.getConnectionStateListenable().removeListener(connectionStateListener);
            executorService.shutdownNow();
            session.close();
        }
    }

    public ObjectMapper getMapper()
    {
        return mapper;
    }

    public ObjectWriter getWriter()
    {
        return writer;
    }

    private void handleLostConnection()
    {
        log.warn("Connection lost - closing all REST sessions");
        session.closeThings();
    }
}
