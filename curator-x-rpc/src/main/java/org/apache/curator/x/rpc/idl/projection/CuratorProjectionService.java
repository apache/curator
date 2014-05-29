
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
import org.apache.curator.framework.api.*;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.x.rpc.connections.Closer;
import org.apache.curator.x.rpc.connections.ConnectionManager;
import org.apache.curator.x.rpc.connections.CuratorEntry;
import org.apache.curator.x.rpc.details.RpcBackgroundCallback;
import org.apache.curator.x.rpc.details.RpcWatcher;
import org.apache.curator.x.rpc.idl.event.OptionalChildrenList;
import org.apache.curator.x.rpc.idl.event.OptionalPath;
import org.apache.curator.x.rpc.idl.event.OptionalRpcStat;
import org.apache.curator.x.rpc.idl.event.RpcCuratorEvent;
import org.apache.curator.x.rpc.idl.event.RpcStat;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@ThriftService("CuratorService")
public class CuratorProjectionService
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final ConnectionManager connectionManager;

    public CuratorProjectionService(ConnectionManager connectionManager)
    {
        this.connectionManager = connectionManager;
    }

    @ThriftMethod
    public CuratorProjection newCuratorProjection(String connectionName)
    {
        CuratorFramework client = connectionManager.newConnection(connectionName);

        String id = UUID.randomUUID().toString();
        client.start();
        connectionManager.add(id, client);
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

    @ThriftMethod
    public void closeCuratorProjection(CuratorProjection projection)
    {
        CuratorEntry entry = connectionManager.remove(projection.id);
        if ( entry != null )
        {
            entry.close();
        }
    }

    @ThriftMethod
    public OptionalPath createNode(CuratorProjection projection, CreateSpec spec) throws Exception
    {
        CuratorFramework client = getEntry(projection).getClient();

        Object builder = client.create();
        if ( spec.creatingParentsIfNeeded )
        {
            builder = castBuilder(builder, CreateBuilder.class).creatingParentsIfNeeded();
        }
        if ( spec.compressed )
        {
            builder = castBuilder(builder, Compressible.class).compressed();
        }
        if ( spec.withProtection )
        {
            builder = castBuilder(builder, CreateBuilder.class).withProtection();
        }
        if ( spec.mode != null )
        {
            builder = castBuilder(builder, CreateModable.class).withMode(getRealMode(spec.mode));
        }

        if ( spec.asyncContext != null )
        {
            BackgroundCallback backgroundCallback = new RpcBackgroundCallback(this, projection);
            builder = castBuilder(builder, Backgroundable.class).inBackground(backgroundCallback, spec.asyncContext);
        }

        Object path = castBuilder(builder, PathAndBytesable.class).forPath(spec.path, spec.data);
        return new OptionalPath((path != null) ? String.valueOf(path) : null);
    }

    @ThriftMethod
    public void deleteNode(CuratorProjection projection, DeleteSpec spec) throws Exception
    {
        CuratorFramework client = getEntry(projection).getClient();

        Object builder = client.delete();
        if ( spec.guaranteed )
        {
            builder = castBuilder(builder, DeleteBuilder.class).guaranteed();
        }
        if ( spec.version != null )
        {
            builder = castBuilder(builder, Versionable.class).withVersion(spec.version.version);
        }

        if ( spec.asyncContext != null )
        {
            BackgroundCallback backgroundCallback = new RpcBackgroundCallback(this, projection);
            builder = castBuilder(builder, Backgroundable.class).inBackground(backgroundCallback, spec.asyncContext);
        }

        castBuilder(builder, Pathable.class).forPath(spec.path);
    }

    @ThriftMethod
    public byte[] getData(CuratorProjection projection, GetDataSpec spec) throws Exception
    {
        CuratorFramework client = getEntry(projection).getClient();

        Object builder = client.getData();
        if ( spec.watched )
        {
            builder = castBuilder(builder, Watchable.class).usingWatcher(new RpcWatcher(this, projection));
        }

        if ( spec.decompressed )
        {
            builder = castBuilder(builder, Decompressible.class).decompressed();
        }

        if ( spec.asyncContext != null )
        {
            BackgroundCallback backgroundCallback = new RpcBackgroundCallback(this, projection);
            builder = castBuilder(builder, Backgroundable.class).inBackground(backgroundCallback);
        }

        Stat stat = new Stat();
        builder = castBuilder(builder, Statable.class).storingStatIn(stat);

        return (byte[])castBuilder(builder, Pathable.class).forPath(spec.path);
    }

    @ThriftMethod
    public RpcStat setData(CuratorProjection projection, SetDataSpec spec) throws Exception
    {
        CuratorFramework client = getEntry(projection).getClient();

        Object builder = client.setData();
        if ( spec.watched )
        {
            builder = castBuilder(builder, Watchable.class).usingWatcher(new RpcWatcher(this, projection));
        }
        if ( spec.version != null )
        {
            builder = castBuilder(builder, Versionable.class).withVersion(spec.version.version);
        }

        if ( spec.compressed )
        {
            builder = castBuilder(builder, Compressible.class).compressed();
        }

        if ( spec.asyncContext != null )
        {
            BackgroundCallback backgroundCallback = new RpcBackgroundCallback(this, projection);
            builder = castBuilder(builder, Backgroundable.class).inBackground(backgroundCallback);
        }

        Stat stat = (Stat)castBuilder(builder, PathAndBytesable.class).forPath(spec.path, spec.data);
        return RpcCuratorEvent.toRpcStat(stat);
    }

    @ThriftMethod
    public OptionalRpcStat exists(CuratorProjection projection, ExistsSpec spec) throws Exception
    {
        CuratorFramework client = getEntry(projection).getClient();

        Object builder = client.checkExists();
        if ( spec.watched )
        {
            builder = castBuilder(builder, Watchable.class).usingWatcher(new RpcWatcher(this, projection));
        }

        if ( spec.asyncContext != null )
        {
            BackgroundCallback backgroundCallback = new RpcBackgroundCallback(this, projection);
            castBuilder(builder, Backgroundable.class).inBackground(backgroundCallback);
        }

        Stat stat = (Stat)castBuilder(builder, Pathable.class).forPath(spec.path);
        return new OptionalRpcStat((stat != null) ? RpcCuratorEvent.toRpcStat(stat) : null);
    }

    @ThriftMethod
    public OptionalChildrenList getChildren(CuratorProjection projection, GetChildrenSpec spec) throws Exception
    {
        CuratorFramework client = getEntry(projection).getClient();

        Object builder = client.getChildren();
        if ( spec.watched )
        {
            builder = castBuilder(builder, Watchable.class).usingWatcher(new RpcWatcher(this, projection));
        }

        if ( spec.asyncContext != null )
        {
            BackgroundCallback backgroundCallback = new RpcBackgroundCallback(this, projection);
            builder = castBuilder(builder, Backgroundable.class).inBackground(backgroundCallback);
        }

        @SuppressWarnings("unchecked")
        List<String> children = (List<String>)castBuilder(builder, Pathable.class).forPath(spec.path);
        return new OptionalChildrenList(children);
    }

    @ThriftMethod
    public boolean closeGenericProjection(CuratorProjection curatorProjection, GenericProjection genericProjection) throws Exception
    {
        CuratorEntry entry = getEntry(curatorProjection);
        return entry.closeThing(genericProjection.id);
    }

    @ThriftMethod
    public GenericProjection acquireLock(CuratorProjection projection, final String path, int maxWaitMs) throws Exception
    {
        CuratorEntry entry = getEntry(projection);
        InterProcessSemaphoreMutex lock = new InterProcessSemaphoreMutex(entry.getClient(), path);
        if ( !lock.acquire(maxWaitMs, TimeUnit.MILLISECONDS) )
        {
            return null;    // TODO
        }

        Closer<InterProcessSemaphoreMutex> closer = new Closer<InterProcessSemaphoreMutex>()
        {
            @Override
            public void close(InterProcessSemaphoreMutex mutex)
            {
                if ( mutex.isAcquiredInThisProcess() )
                {
                    try
                    {
                        mutex.release();
                    }
                    catch ( Exception e )
                    {
                        log.error("Could not release left-over lock for path: " + path, e);
                    }
                }
            }
        };
        String id = entry.addThing(lock, closer);
        return new GenericProjection(id);
    }

    public void addEvent(CuratorProjection projection, RpcCuratorEvent event)
    {
        CuratorEntry entry = connectionManager.get(projection.id);
        if ( entry != null )
        {
            entry.addEvent(event);
        }
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

    private CuratorEntry getEntry(CuratorProjection projection) throws Exception
    {
        CuratorEntry entry = connectionManager.get(projection.id);
        if ( entry == null )
        {
            throw new Exception("No client found with id: " + projection.id);
        }
        return entry;
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
