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
import org.apache.curator.framework.listen.ListenerContainer;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.x.rest.api.NodeCacheResource;
import org.apache.curator.x.rest.entities.NodeCacheSpec;
import org.apache.curator.x.rest.entities.NodeData;
import org.apache.curator.x.rest.entities.OptionalNodeData;
import org.apache.curator.x.rest.entities.PathAndId;
import org.apache.curator.x.rest.entities.Status;
import org.apache.curator.x.rest.entities.StatusMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.annotation.Nullable;
import javax.ws.rs.core.MediaType;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public class NodeCacheBridge implements Closeable, StatusListener
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final ListenerContainer<NodeCacheListener> listeners = new ListenerContainer<NodeCacheListener>();
    private final Client restClient;
    private final SessionManager sessionManager;
    private final UriMaker uriMaker;
    private final String path;

    private volatile String id;

    public NodeCacheBridge(Client restClient, SessionManager sessionManager, UriMaker uriMaker, String path)
    {
        this.restClient = restClient;
        this.sessionManager = sessionManager;
        this.uriMaker = uriMaker;
        this.path = path;
    }

    @Override
    public void close() throws IOException
    {
        sessionManager.removeEntry(uriMaker.getLocalhost(), id);
        restClient.resource(uriMaker.getMethodUri(NodeCacheResource.class, null)).path(id).delete();
    }

    public void start(boolean buildInitial)
    {
        NodeCacheSpec nodeCacheSpec = new NodeCacheSpec();
        nodeCacheSpec.setPath(path);
        nodeCacheSpec.setBuildInitial(buildInitial);
        id = restClient.resource(uriMaker.getMethodUri(NodeCacheResource.class, null)).type(MediaType.APPLICATION_JSON).post(PathAndId.class, nodeCacheSpec).getId();
        sessionManager.addEntry(uriMaker.getLocalhost(), id, this);
    }

    public ListenerContainer<NodeCacheListener> getListenable()
    {
        return listeners;
    }

    public OptionalNodeData getCurrentData() throws Exception
    {
        return restClient.resource(uriMaker.getMethodUri(NodeCacheResource.class, null)).path(id).type(MediaType.APPLICATION_JSON).get(OptionalNodeData.class);
    }

    @Override
    public void statusUpdate(List<StatusMessage> messages)
    {
        for ( StatusMessage statusMessage : messages )
        {
            if ( statusMessage.getType().equals("node-cache") && statusMessage.getSourceId().equals(id) )
            {
                listeners.forEach(new Function<NodeCacheListener, Void>()
                {
                    @Nullable
                    @Override
                    public Void apply(NodeCacheListener listener)
                    {
                        try
                        {
                            listener.nodeChanged();
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
