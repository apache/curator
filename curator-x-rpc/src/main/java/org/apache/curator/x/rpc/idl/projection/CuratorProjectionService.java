
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
package org.apache.curator.x.rpc.idl.projection;

import com.facebook.swift.service.ThriftMethod;
import com.facebook.swift.service.ThriftService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.Backgroundable;
import org.apache.curator.framework.api.Compressible;
import org.apache.curator.framework.api.CreateBuilder;
import org.apache.curator.framework.api.CreateModable;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.PathAndBytesable;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.x.rpc.CuratorEntry;
import org.apache.curator.x.rpc.RpcManager;
import org.apache.curator.x.rpc.idl.event.RpcCuratorEvent;
import java.util.UUID;

@ThriftService("CuratorService")
public class CuratorProjectionService
{
    private final RpcManager rpcManager;

    public CuratorProjectionService(RpcManager rpcManager)
    {
        this.rpcManager = rpcManager;
    }

    @ThriftMethod
    public CuratorProjection newCuratorProjection(CuratorProjectionSpec spec)   // TODO
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient("localhost:2181", new RetryOneTime(1));
        String id = UUID.randomUUID().toString();
        client.start();
        rpcManager.add(id, client);
        final CuratorProjection projection = new CuratorProjection(id);

        ConnectionStateListener listener = new ConnectionStateListener()
        {
            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState)
            {
                addEvent(projection, new RpcCuratorEvent(newState));
            }
        };
        client.getConnectionStateListenable().addListener(listener);

        return projection;
    }

    private void addEvent(CuratorProjection projection, RpcCuratorEvent event)
    {
        CuratorEntry entry = rpcManager.get(projection.id);
        if ( entry != null )
        {
            entry.addEvent(event);
        }
    }

    @ThriftMethod
    public void closeCuratorProjection(CuratorProjection projection)
    {
        CuratorEntry entry = rpcManager.remove(projection.id);
        if ( entry != null )
        {
            entry.close();
        }
    }

    @ThriftMethod
    public String create(final CuratorProjection projection, CreateSpec createSpec) throws Exception
    {
        CuratorFramework client = getClient(projection);

        Object builder = client.create();
        if ( createSpec.creatingParentsIfNeeded )
        {
            builder = castBuilder(builder, CreateBuilder.class).creatingParentsIfNeeded();
        }
        if ( createSpec.compressed )
        {
            builder = castBuilder(builder, Compressible.class).compressed();
        }
        if ( createSpec.withProtection )
        {
            builder = castBuilder(builder, CreateBuilder.class).withProtection();
        }
        if ( createSpec.mode != null )
        {
            builder = castBuilder(builder, CreateModable.class).withMode(getRealMode(createSpec.mode));
        }

        if ( createSpec.asyncContext != null )
        {
            BackgroundCallback backgroundCallback = new BackgroundCallback()
            {
                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
                {
                    addEvent(projection, new RpcCuratorEvent(event));
                }
            };
            builder = castBuilder(builder, Backgroundable.class).inBackground(backgroundCallback, createSpec.asyncContext);
        }

        return String.valueOf(castBuilder(builder, PathAndBytesable.class).forPath(createSpec.path, createSpec.data));
    }

    private org.apache.zookeeper.CreateMode getRealMode(CreateMode mode)
    {
        switch ( mode )
        {
            case PERSISTENT:
            {
                return org.apache.zookeeper.CreateMode.PERSISTENT;
            }

            case PERSISTENT_SEQUENTIAL:
            {
                return org.apache.zookeeper.CreateMode.PERSISTENT_SEQUENTIAL;
            }

            case EPHEMERAL:
            {
                return org.apache.zookeeper.CreateMode.EPHEMERAL;
            }

            case EPHEMERAL_SEQUENTIAL:
            {
                return org.apache.zookeeper.CreateMode.EPHEMERAL_SEQUENTIAL;
            }
        }
        throw new UnsupportedOperationException("Bad mode: " + mode.toString());
    }

    private CuratorFramework getClient(CuratorProjection projection) throws Exception
    {
        CuratorEntry entry = rpcManager.get(projection.id);
        if ( entry == null )
        {
            throw new Exception("No client found with id: " + projection.id);
        }
        return entry.getClient();
    }

    private static <T> T castBuilder(Object createBuilder, Class<T> clazz) throws Exception
    {
        if ( clazz.isAssignableFrom(createBuilder.getClass()) )
        {
            return clazz.cast(createBuilder);
        }
        throw new Exception("That operation is not available"); // TODO
    }
}
