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

package org.apache.curator.x.rest.support;

import com.google.common.base.Equivalence;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.sun.jersey.api.client.Client;
import org.apache.curator.x.rest.api.ClientResource;
import org.apache.curator.x.rest.entities.Status;
import org.apache.curator.x.rest.entities.StatusMessage;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriBuilder;
import java.io.Closeable;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SessionManager implements Closeable
{
    private final Client restClient;
    private final Future<?> task;
    private final ConcurrentMap<InetSocketAddress, Entry> entries = Maps.newConcurrentMap();

    private volatile StatusListener statusListener;

    private static class Entry
    {
        private final StatusListener listener;
        private final Queue<String> ids;

        private Entry(StatusListener listener, Queue<String> ids)
        {
            this.listener = listener;
            this.ids = ids;
        }
    }

    public SessionManager(Client restClient, int sessionMs)
    {
        this(restClient, Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setDaemon(true).setNameFormat("SessionManager-%d").build()), sessionMs);
    }

    public SessionManager(Client restClient, ScheduledExecutorService service, int sessionMs)
    {
        this.restClient = restClient;
        Runnable command = new Runnable()
        {
            @Override
            public void run()
            {
                processEntries();
            }
        };
        task = service.scheduleAtFixedRate(command, sessionMs / 3, sessionMs / 3, TimeUnit.MILLISECONDS);
    }

    public void addEntry(InetSocketAddress address, String id, StatusListener listener)
    {
        Entry newEntry = new Entry(listener, new ConcurrentLinkedQueue<String>());
        Entry oldEntry = entries.putIfAbsent(address, newEntry);
        Entry useEntry = (oldEntry != null) ? oldEntry : newEntry;
        Preconditions.checkArgument(Equivalence.identity().equivalent(useEntry.listener, listener), "Different listener passed in for address: " + address);
        useEntry.ids.add(id);
    }

    public void removeEntry(InetSocketAddress address, String id)
    {
        Entry entry = entries.get(address);
        if ( entry != null )
        {
            entry.ids.remove(id);
        }
    }

    @Override
    public void close()
    {
        task.cancel(true);
    }

    public void setStatusListener(StatusListener statusListener)
    {
        Preconditions.checkState(this.statusListener == null, "statusListener already set");
        this.statusListener = statusListener;
    }

    private void processEntries()
    {
        for ( Map.Entry<InetSocketAddress, Entry> mapEntry : entries.entrySet() )
        {
            InetSocketAddress address = mapEntry.getKey();
            Entry entry = mapEntry.getValue();

            URI uri = UriBuilder
                .fromUri("http://" + address.getHostName() + ":" + address.getPort())
                .path(ClientResource.class)
                .path(ClientResource.class, "getStatus")
                .build();

            List<String> idsList = Lists.newArrayList(entry.ids);
            Status status = restClient.resource(uri).type(MediaType.APPLICATION_JSON).post(Status.class, idsList);
            if ( status.getState().equals("connected") )
            {
                List<StatusMessage> messages = status.getMessages();
                if ( messages.size() > 0 )
                {
                    if ( statusListener != null )
                    {
                        statusListener.statusUpdate(status.getMessages());
                    }
                    if ( entry.listener != null )
                    {
                        entry.listener.statusUpdate(status.getMessages());
                    }
                }
            }
            else
            {
                if ( statusListener != null )
                {
                    statusListener.errorState(status);
                }
                if ( entry.listener != null )
                {
                    entry.listener.errorState(status);
                }
            }
        }
    }
}
