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
import org.apache.curator.utils.ThreadUtils;
import org.apache.curator.x.rest.api.Session;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;
import java.io.Closeable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class CuratorRestContext implements Closeable
{
    private final Session session = new Session();
    private final ObjectMapper mapper = new ObjectMapper();
    private final ObjectWriter writer = mapper.writer();
    private final CuratorFramework client;
    private final int sessionLengthMs;
    private final AtomicReference<State> state = new AtomicReference<State>(State.LATENT);
    private final ScheduledExecutorService executorService = ThreadUtils.newSingleThreadScheduledExecutor("CuratorRestContext");

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
        return session;
    }

    public void start()
    {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "Already started");

        Runnable runner = new Runnable()
        {
            @Override
            public void run()
            {
                session.checkExpiredThings(sessionLengthMs);
            }
        };
        executorService.scheduleAtFixedRate(runner, sessionLengthMs, sessionLengthMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close()
    {
        if ( state.compareAndSet(State.STARTED, State.CLOSED) )
        {
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
}
