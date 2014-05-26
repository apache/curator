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
import com.google.common.collect.Maps;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.Backgroundable;
import org.apache.curator.framework.api.Compressible;
import org.apache.curator.framework.api.CreateBuilder;
import org.apache.curator.framework.api.CreateModable;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.PathAndBytesable;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.x.rpc.idl.event.EventService;
import org.apache.curator.x.rpc.idl.event.RpcCuratorEvent;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@ThriftService("CuratorService")
public class CuratorProjectionService
{
    private final Map<String, CuratorFramework> projections = Maps.newConcurrentMap();
    private final EventService eventService;

    public CuratorProjectionService(EventService eventService)
    {
        this.eventService = eventService;
    }

    @ThriftMethod
    public CuratorProjection newCuratorProjection(CuratorProjectionSpec spec)   // TODO
    {
        System.out.println(Thread.currentThread() + "newCuratorProjection");

        eventService.addEvent(new RpcCuratorEvent(null, new CuratorEvent()
        {
            @Override
            public CuratorEventType getType()
            {
                return CuratorEventType.CHILDREN;
            }

            @Override
            public int getResultCode()
            {
                return 1;
            }

            @Override
            public String getPath()
            {
                return null;
            }

            @Override
            public Object getContext()
            {
                return null;
            }

            @Override
            public Stat getStat()
            {
                return null;
            }

            @Override
            public byte[] getData()
            {
                return new byte[0];
            }

            @Override
            public String getName()
            {
                return null;
            }

            @Override
            public List<String> getChildren()
            {
                return null;
            }

            @Override
            public List<ACL> getACLList()
            {
                return null;
            }

            @Override
            public WatchedEvent getWatchedEvent()
            {
                return null;
            }
        }));

        CuratorFramework client = CuratorFrameworkFactory.newClient("localhost:2181", new RetryOneTime(1));
        String id = UUID.randomUUID().toString();
        client.start();
        projections.put(id, client);
        return new CuratorProjection(id);
    }

    @ThriftMethod
    public void closeCuratorProjection(CuratorProjection projection)
    {
        CuratorFramework client = projections.remove(projection.id);
        if ( client != null )
        {
            client.close();
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
        builder = castBuilder(builder, CreateModable.class).withMode(getRealMode(createSpec.mode));

        if ( createSpec.doAsync )
        {
            BackgroundCallback backgroundCallback = new BackgroundCallback()
            {
                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
                {
                    eventService.addEvent(new RpcCuratorEvent(projection, event));
                }
            };
            builder = castBuilder(builder, Backgroundable.class).inBackground(backgroundCallback);
        }

        return String.valueOf(castBuilder(builder, PathAndBytesable.class).forPath(createSpec.path, createSpec.data.getBytes()));
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
        CuratorFramework client = projections.get(projection.id);
        if ( client == null )
        {
            throw new Exception("No client found with id: " + projection.id);
        }
        return client;
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
