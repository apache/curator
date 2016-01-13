
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
package org.apache.curator.x.rpc.idl.services;

import com.facebook.swift.service.ThriftMethod;
import com.facebook.swift.service.ThriftService;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.*;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreV2;
import org.apache.curator.framework.recipes.locks.Lease;
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.utils.ThreadUtils;
import org.apache.curator.x.rpc.connections.Closer;
import org.apache.curator.x.rpc.connections.ConnectionManager;
import org.apache.curator.x.rpc.connections.CuratorEntry;
import org.apache.curator.x.rpc.details.RpcBackgroundCallback;
import org.apache.curator.x.rpc.details.RpcWatcher;
import org.apache.curator.x.rpc.idl.exceptions.ExceptionType;
import org.apache.curator.x.rpc.idl.exceptions.RpcException;
import org.apache.curator.x.rpc.idl.structs.*;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
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
    public CuratorProjection newCuratorProjection(String connectionName) throws RpcException
    {
        CuratorFramework client = connectionManager.newConnection(connectionName);
        if ( client == null )
        {
            throw new RpcException(ExceptionType.GENERAL, null, null, "No connection configuration was found with the name: " + connectionName);
        }

        String id = CuratorEntry.newId();
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

    @ThriftMethod(oneway = true)
    public void pingCuratorProjection(CuratorProjection projection)
    {
        connectionManager.get(projection.id);
    }

    @ThriftMethod
    public OptionalPath createNode(CuratorProjection projection, CreateSpec spec) throws RpcException
    {
        try
        {
            CuratorFramework client = CuratorEntry.mustGetEntry(connectionManager, projection).getClient();

            Object builder = client.create();
            if ( spec.creatingParentsIfNeeded )
            {
                builder = castBuilder(builder, CreateBuilder.class).creatingParentsIfNeeded();
            }
            if ( spec.creatingParentContainersIfNeeded )
            {
                builder = castBuilder(builder, CreateBuilder.class).creatingParentContainersIfNeeded();
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
                builder = castBuilder(builder, CreateModable.class).withMode(CreateMode.valueOf(spec.mode.name()));
            }

            if ( spec.asyncContext != null )
            {
                BackgroundCallback backgroundCallback = new RpcBackgroundCallback(this, projection);
                builder = castBuilder(builder, Backgroundable.class).inBackground(backgroundCallback, spec.asyncContext);
            }

            Object path = castBuilder(builder, PathAndBytesable.class).forPath(spec.path, spec.data);
            return new OptionalPath((path != null) ? String.valueOf(path) : null);
        }
        catch ( Exception e )
        {
            ThreadUtils.checkInterrupted(e);
            throw new RpcException(e);
        }
    }

    @ThriftMethod
    public void deleteNode(CuratorProjection projection, DeleteSpec spec) throws RpcException
    {
        try
        {
            CuratorFramework client = CuratorEntry.mustGetEntry(connectionManager, projection).getClient();

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
        catch ( Exception e )
        {
            ThreadUtils.checkInterrupted(e);
            throw new RpcException(e);
        }
    }

    @ThriftMethod
    public OptionalData getData(CuratorProjection projection, GetDataSpec spec) throws RpcException
    {
        try
        {
            CuratorFramework client = CuratorEntry.mustGetEntry(connectionManager, projection).getClient();

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

            byte[] bytes = (byte[])castBuilder(builder, Pathable.class).forPath(spec.path);
            return new OptionalData(bytes);
        }
        catch ( Exception e )
        {
            ThreadUtils.checkInterrupted(e);
            throw new RpcException(e);
        }
    }

    @ThriftMethod
    public OptionalRpcStat setData(CuratorProjection projection, SetDataSpec spec) throws RpcException
    {
        try
        {
            CuratorFramework client = CuratorEntry.mustGetEntry(connectionManager, projection).getClient();

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
            return new OptionalRpcStat(RpcCuratorEvent.toRpcStat(stat));
        }
        catch ( Exception e )
        {
            ThreadUtils.checkInterrupted(e);
            throw new RpcException(e);
        }
    }

    @ThriftMethod
    public OptionalRpcStat exists(CuratorProjection projection, ExistsSpec spec) throws RpcException
    {
        try
        {
            CuratorFramework client = CuratorEntry.mustGetEntry(connectionManager, projection).getClient();

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
        catch ( Exception e )
        {
            ThreadUtils.checkInterrupted(e);
            throw new RpcException(e);
        }
    }

    @ThriftMethod
    public OptionalChildrenList getChildren(CuratorProjection projection, GetChildrenSpec spec) throws RpcException
    {
        try
        {
            CuratorFramework client = CuratorEntry.mustGetEntry(connectionManager, projection).getClient();

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
        catch ( Exception e )
        {
            ThreadUtils.checkInterrupted(e);
            throw new RpcException(e);
        }
    }

    @ThriftMethod
    public void sync(CuratorProjection projection, String path, String asyncContext) throws RpcException
    {
        try
        {
            CuratorEntry entry = CuratorEntry.mustGetEntry(connectionManager, projection);
            BackgroundCallback backgroundCallback = new RpcBackgroundCallback(this, projection);
            entry.getClient().sync().inBackground(backgroundCallback, asyncContext).forPath(path);
        }
        catch ( Exception e )
        {
            ThreadUtils.checkInterrupted(e);
            throw new RpcException(e);
        }
    }

    @ThriftMethod
    public boolean closeGenericProjection(CuratorProjection projection, String id) throws RpcException
    {
        try
        {
            if ( id.equals(projection.id) )
            {
                closeCuratorProjection(projection);
                return true;
            }
            else
            {
                CuratorEntry entry = CuratorEntry.mustGetEntry(connectionManager, projection);
                return entry.closeThing(id);
            }
        }
        catch ( Exception e )
        {
            ThreadUtils.checkInterrupted(e);
            throw new RpcException(e);
        }
    }

    @ThriftMethod
    public OptionalLockProjection acquireLock(CuratorProjection projection, final String path, int maxWaitMs) throws RpcException
    {
        try
        {
            CuratorEntry entry = CuratorEntry.mustGetEntry(connectionManager, projection);
            final InterProcessSemaphoreMutex lock = new InterProcessSemaphoreMutex(entry.getClient(), path);
            if ( !lock.acquire(maxWaitMs, TimeUnit.MILLISECONDS) )
            {
                return new OptionalLockProjection();
            }

            Closer closer = new Closer()
            {
                @Override
                public void close()
                {
                    if ( lock.isAcquiredInThisProcess() )
                    {
                        try
                        {
                            lock.release();
                        }
                        catch ( Exception e )
                        {
                            ThreadUtils.checkInterrupted(e);
                            log.error("Could not release left-over lock for path: " + path, e);
                        }
                    }
                }
            };
            String id = entry.addThing(lock, closer);
            return new OptionalLockProjection(new LockProjection(id));
        }
        catch ( Exception e )
        {
            ThreadUtils.checkInterrupted(e);
            throw new RpcException(e);
        }
    }

    @ThriftMethod
    public LeaderResult startLeaderSelector(final CuratorProjection projection, final String path, final String participantId, int waitForLeadershipMs) throws RpcException
    {
        try
        {
            CuratorEntry entry = CuratorEntry.mustGetEntry(connectionManager, projection);

            final LeaderLatch leaderLatch = new LeaderLatch(entry.getClient(), path, participantId);
            leaderLatch.start();

            Closer closer = new Closer()
            {
                @Override
                public void close()
                {
                    try
                    {
                        leaderLatch.close();
                    }
                    catch ( IOException e )
                    {
                        ThreadUtils.checkInterrupted(e);
                        log.error("Could not close left-over leader latch for path: " + path, e);
                    }
                }
            };
            String id = entry.addThing(leaderLatch, closer);

            LeaderLatchListener listener = new LeaderLatchListener()
            {
                @Override
                public void isLeader()
                {
                    addEvent(projection, new RpcCuratorEvent(new LeaderEvent(path, participantId, true)));
                }

                @Override
                public void notLeader()
                {
                    addEvent(projection, new RpcCuratorEvent(new LeaderEvent(path, participantId, false)));
                }
            };
            leaderLatch.addListener(listener);

            if ( waitForLeadershipMs > 0 )
            {
                leaderLatch.await(waitForLeadershipMs, TimeUnit.MILLISECONDS);
            }

            return new LeaderResult(new LeaderProjection(id), leaderLatch.hasLeadership());
        }
        catch ( Exception e )
        {
            ThreadUtils.checkInterrupted(e);
            throw new RpcException(e);
        }
    }

    @ThriftMethod
    public Collection<RpcParticipant> getLeaderParticipants(CuratorProjection projection, LeaderProjection leaderProjection) throws RpcException
    {
        try
        {
            CuratorEntry entry = CuratorEntry.mustGetEntry(connectionManager, projection);

            LeaderLatch leaderLatch = CuratorEntry.mustGetThing(entry, leaderProjection.id, LeaderLatch.class);
            Collection<Participant> participants = leaderLatch.getParticipants();
            Collection<RpcParticipant> transformed = Collections2.transform
            (
                participants,
                new Function<Participant, RpcParticipant>()
                {
                    @Override
                    public RpcParticipant apply(Participant participant)
                    {
                        return new RpcParticipant(participant.getId(), participant.isLeader());
                    }
                }
            );
            return Lists.newArrayList(transformed);
        }
        catch ( Exception e )
        {
            ThreadUtils.checkInterrupted(e);
            throw new RpcException(e);
        }
    }

    @ThriftMethod
    public boolean isLeader(CuratorProjection projection, LeaderProjection leaderProjection) throws RpcException
    {
        try
        {
            CuratorEntry entry = CuratorEntry.mustGetEntry(connectionManager, projection);

            LeaderLatch leaderLatch = CuratorEntry.mustGetThing(entry, leaderProjection.id, LeaderLatch.class);
            return leaderLatch.hasLeadership();
        }
        catch ( Exception e )
        {
            ThreadUtils.checkInterrupted(e);
            throw new RpcException(e);
        }
    }

    @ThriftMethod
    public PathChildrenCacheProjection startPathChildrenCache(final CuratorProjection projection, final String path, boolean cacheData, boolean dataIsCompressed, PathChildrenCacheStartMode startMode) throws RpcException
    {
        try
        {
            final CuratorEntry entry = CuratorEntry.mustGetEntry(connectionManager, projection);

            final PathChildrenCache cache = new PathChildrenCache(entry.getClient(), path, cacheData, dataIsCompressed, ThreadUtils.newThreadFactory("PathChildrenCacheResource"));
            cache.start(PathChildrenCache.StartMode.valueOf(startMode.name()));

            Closer closer = new Closer()
            {
                @Override
                public void close()
                {
                    try
                    {
                        cache.close();
                    }
                    catch ( IOException e )
                    {
                        ThreadUtils.checkInterrupted(e);
                        log.error("Could not close left-over PathChildrenCache for path: " + path, e);
                    }
                }
            };
            String id = entry.addThing(cache, closer);

            PathChildrenCacheListener listener = new PathChildrenCacheListener()
            {
                @Override
                public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws RpcException
                {
                    entry.addEvent(new RpcCuratorEvent(new RpcPathChildrenCacheEvent(path, event)));
                }
            };
            cache.getListenable().addListener(listener);

            return new PathChildrenCacheProjection(id);
        }
        catch ( Exception e )
        {
            ThreadUtils.checkInterrupted(e);
            throw new RpcException(e);
        }
    }

    @ThriftMethod
    public List<RpcChildData> getPathChildrenCacheData(CuratorProjection projection, PathChildrenCacheProjection cacheProjection) throws RpcException
    {
        try
        {
            CuratorEntry entry = CuratorEntry.mustGetEntry(connectionManager, projection);

            PathChildrenCache pathChildrenCache = CuratorEntry.mustGetThing(entry, cacheProjection.id, PathChildrenCache.class);
            return Lists.transform
            (
                pathChildrenCache.getCurrentData(),
                new Function<ChildData, RpcChildData>()
                {
                    @Override
                    public RpcChildData apply(ChildData childData)
                    {
                        return new RpcChildData(childData);
                    }
                }
            );
        }
        catch ( Exception e )
        {
            ThreadUtils.checkInterrupted(e);
            throw new RpcException(e);
        }
    }

    @ThriftMethod
    public RpcChildData getPathChildrenCacheDataForPath(CuratorProjection projection, PathChildrenCacheProjection cacheProjection, String path) throws RpcException
    {
        try
        {
            CuratorEntry entry = CuratorEntry.mustGetEntry(connectionManager, projection);

            PathChildrenCache pathChildrenCache = CuratorEntry.mustGetThing(entry, cacheProjection.id, PathChildrenCache.class);
            return new RpcChildData(pathChildrenCache.getCurrentData(path));
        }
        catch ( Exception e )
        {
            ThreadUtils.checkInterrupted(e);
            throw new RpcException(e);
        }
    }

    @ThriftMethod
    public NodeCacheProjection startNodeCache(CuratorProjection projection, final String path, boolean dataIsCompressed, boolean buildInitial) throws RpcException
    {
        try
        {
            final CuratorEntry entry = CuratorEntry.mustGetEntry(connectionManager, projection);

            final NodeCache cache = new NodeCache(entry.getClient(), path, dataIsCompressed);
            cache.start(buildInitial);

            Closer closer = new Closer()
            {
                @Override
                public void close()
                {
                    try
                    {
                        cache.close();
                    }
                    catch ( IOException e )
                    {
                        ThreadUtils.checkInterrupted(e);
                        log.error("Could not close left-over NodeCache for path: " + path, e);
                    }
                }
            };
            String id = entry.addThing(cache, closer);

            NodeCacheListener listener = new NodeCacheListener()
            {
                @Override
                public void nodeChanged()
                {
                    entry.addEvent(new RpcCuratorEvent(RpcCuratorEventType.NODE_CACHE, path));
                }
            };
            cache.getListenable().addListener(listener);

            return new NodeCacheProjection(id);
        }
        catch ( Exception e )
        {
            ThreadUtils.checkInterrupted(e);
            throw new RpcException(e);
        }
    }

    @ThriftMethod
    public RpcChildData getNodeCacheData(CuratorProjection projection, NodeCacheProjection cacheProjection) throws RpcException
    {
        try
        {
            CuratorEntry entry = CuratorEntry.mustGetEntry(connectionManager, projection);

            NodeCache nodeCache = CuratorEntry.mustGetThing(entry, cacheProjection.id, NodeCache.class);
            return new RpcChildData(nodeCache.getCurrentData());
        }
        catch ( Exception e )
        {
            ThreadUtils.checkInterrupted(e);
            throw new RpcException(e);
        }
    }

    @ThriftMethod
    public PersistentEphemeralNodeProjection startPersistentEphemeralNode(CuratorProjection projection, final String path, byte[] data, RpcPersistentEphemeralNodeMode mode) throws RpcException
    {
        try
        {
            CuratorEntry entry = CuratorEntry.mustGetEntry(connectionManager, projection);

            final PersistentEphemeralNode node = new PersistentEphemeralNode(entry.getClient(), PersistentEphemeralNode.Mode.valueOf(mode.name()), path, data);
            node.start();

            Closer closer = new Closer()
            {
                @Override
                public void close()
                {
                    try
                    {
                        node.close();
                    }
                    catch ( Exception e )
                    {
                        ThreadUtils.checkInterrupted(e);
                        log.error("Could not release left-over persistent ephemeral node for path: " + path, e);
                    }
                }
            };
            String id = entry.addThing(node, closer);
            return new PersistentEphemeralNodeProjection(id);
        }
        catch ( Exception e )
        {
            ThreadUtils.checkInterrupted(e);
            throw new RpcException(e);
        }
    }

    @ThriftMethod
    public List<LeaseProjection> acquireSemaphore(CuratorProjection projection, final String path, int acquireQty, int maxWaitMs, int maxLeases) throws RpcException
    {
        try
        {
            CuratorEntry entry = CuratorEntry.mustGetEntry(connectionManager, projection);

            final InterProcessSemaphoreV2 semaphore = new InterProcessSemaphoreV2(entry.getClient(), path, maxLeases);
            final Collection<Lease> leases = semaphore.acquire(acquireQty, maxWaitMs, TimeUnit.MILLISECONDS);
            if ( leases == null )
            {
                return Lists.newArrayList();
            }

            List<LeaseProjection> leaseProjections = Lists.newArrayList();
            for ( final Lease lease : leases )
            {
                Closer closer = new Closer()
                {
                    @Override
                    public void close()
                    {
                        try
                        {
                            semaphore.returnLease(lease);
                        }
                        catch ( Exception e )
                        {
                            ThreadUtils.checkInterrupted(e);
                            log.error("Could not release semaphore leases for path: " + path, e);
                        }
                    }
                };
                leaseProjections.add(new LeaseProjection(entry.addThing(lease, closer)));
            }
            return leaseProjections;
        }
        catch ( Exception e )
        {
            ThreadUtils.checkInterrupted(e);
            throw new RpcException(e);
        }
    }

    public void addEvent(CuratorProjection projection, RpcCuratorEvent event)
    {
        CuratorEntry entry = connectionManager.get(projection.id);
        if ( entry != null )
        {
            entry.addEvent(event);
        }
    }

    private static <T> T castBuilder(Object createBuilder, Class<T> clazz) throws Exception
    {
        if ( clazz.isAssignableFrom(createBuilder.getClass()) )
        {
            return clazz.cast(createBuilder);
        }
        throw new Exception("That operation is not available");
    }
}
