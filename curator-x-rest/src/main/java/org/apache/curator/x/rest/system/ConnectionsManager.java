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

package org.apache.curator.x.rest.system;

import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ThreadUtils;
import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

// TODO connection cleanup/timeouts

public class ConnectionsManager implements Closeable
{
    private final Map<String, Connection> connections = Maps.newConcurrentMap();
    private final CuratorFrameworkAllocator allocator;
    private final ExecutorService executorService;

    public ConnectionsManager(CuratorFrameworkAllocator allocator)
    {
        this(allocator, Executors.newCachedThreadPool(ThreadUtils.newThreadFactory("ConnectionsManager")));
    }

    public ConnectionsManager(CuratorFrameworkAllocator allocator, ExecutorService executorService)
    {
        this.allocator = allocator;
        this.executorService = executorService;
    }

    public String newConnection() throws Exception
    {
        String id = UUID.randomUUID().toString();
        CuratorFramework client = allocator.newCuratorFramework();
        connections.put(id, new Connection(client));
        return id;
    }

    public Connection get(String id)
    {
        Connection connection = connections.get(id);
        if ( connection != null )
        {
            connection.updateUse();
        }
        return connection;
    }

    public boolean closeConnection(String id)
    {
        Connection connection = connections.remove(id);
        if ( connection != null )
        {
            connection.close();
            return true;
        }
        return false;
    }

    public ExecutorService getExecutorService()
    {
        return executorService;
    }

    @Override
    public void close() throws IOException
    {
        for ( Connection connection : connections.values() )
        {
            Closeables.closeQuietly(connection);
        }
    }
}
