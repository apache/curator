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

import com.google.common.base.Function;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.GenericType;
import org.apache.curator.framework.listen.ListenerContainer;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.x.rest.api.PathChildrenCacheResource;
import org.apache.curator.x.rest.entities.OptionalNodeData;
import org.apache.curator.x.rest.entities.PathAndId;
import org.apache.curator.x.rest.entities.PathChildrenCacheSpec;
import org.apache.curator.x.rest.entities.Status;
import org.apache.curator.x.rest.entities.StatusMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.annotation.Nullable;
import javax.ws.rs.core.MediaType;
import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.List;

public class PathChildrenCacheBridge implements Closeable, StatusListener
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Client restClient;
    private final SessionManager sessionManager;
    private final UriMaker uriMaker;
    private final String path;
    private final boolean cacheData;
    private final boolean dataIsCompressed;
    private final ListenerContainer<Listener> listeners = new ListenerContainer<Listener>();

    private volatile String id;

    public PathChildrenCacheBridge(Client restClient, SessionManager sessionManager, UriMaker uriMaker, String path, boolean cacheData, boolean dataIsCompressed)
    {
        this.restClient = restClient;
        this.sessionManager = sessionManager;
        this.uriMaker = uriMaker;
        this.path = path;
        this.cacheData = cacheData;
        this.dataIsCompressed = dataIsCompressed;
    }

    public void start() throws Exception
    {
        PathChildrenCacheSpec spec = new PathChildrenCacheSpec();
        spec.setPath(path);
        spec.setCacheData(cacheData);
        spec.setDataIsCompressed(dataIsCompressed);
        spec.setStartMode(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
        id = restClient.resource(uriMaker.getMethodUri(PathChildrenCacheResource.class, null)).type(MediaType.APPLICATION_JSON).post(PathAndId.class, spec).getId();
        sessionManager.addEntry(uriMaker.getLocalhost(), id, this);
    }

    @Override
    public void close() throws IOException
    {
        sessionManager.removeEntry(uriMaker.getLocalhost(), id);
        if ( id != null )
        {
            URI uri = uriMaker.getMethodUri(PathChildrenCacheResource.class, null);
            restClient.resource(uri).path(id).delete();
        }
    }

    public interface Listener
    {
        public void childEvent(String event, String path) throws Exception;
    }

    public ListenerContainer<Listener> getListenable()
    {
        return listeners;
    }

    public List<ChildData> getCurrentData()
    {
        GenericType<List<ChildData>> type = new GenericType<List<ChildData>>(){};
        return restClient.resource(uriMaker.getMethodUri(PathChildrenCacheResource.class, "getCacheData")).path(id).type(MediaType.APPLICATION_JSON).get(type);
    }

    public OptionalNodeData getCurrentData(String path)
    {
        URI uri = uriMaker.getMethodUri(PathChildrenCacheResource.class, null);
        return restClient.resource(uri).path(id).path(path).type(MediaType.APPLICATION_JSON).get(OptionalNodeData.class);
    }

    @Override
    public void statusUpdate(List<StatusMessage> messages)
    {
        for ( final StatusMessage statusMessage : messages )
        {
            if ( statusMessage.getType().equals("path-cache") && statusMessage.getSourceId().equals(id) )
            {
                listeners.forEach(new Function<Listener, Void>()
                {
                    @Nullable
                    @Override
                    public Void apply(Listener listener)
                    {
                        try
                        {
                            listener.childEvent(statusMessage.getMessage(), statusMessage.getDetails());
                        }
                        catch ( Exception e )
                        {
                            log.error("Calling listener", e);
                        }
                        return null;
                    }
                });
            }
        }
    }

    @Override
    public void errorState(Status status)
    {
    }
}
