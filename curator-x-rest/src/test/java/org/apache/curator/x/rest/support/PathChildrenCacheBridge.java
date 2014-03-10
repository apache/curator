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

import com.sun.jersey.api.client.Client;
import org.apache.curator.framework.listen.ListenerContainer;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.x.rest.entities.NodeData;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public class PathChildrenCacheBridge implements Closeable
{
    private final Client restClient;
    private final SessionManager sessionManager;
    private final UriMaker uriMaker;
    private final String path;
    private final boolean cacheData;
    private final boolean dataIsCompressed;
    private final ListenerContainer<PathChildrenCacheListener> listeners = new ListenerContainer<PathChildrenCacheListener>();

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

    }

    @Override
    public void close() throws IOException
    {

    }

    public ListenerContainer<PathChildrenCacheListener> getListenable()
    {
        return listeners;
    }

    public List<NodeData> getCurrentData()
    {
        return null;
    }
}
