package com.netflix.curator.x.sync;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.cache.ChildData;
import com.netflix.curator.framework.recipes.cache.NodeCache;
import com.netflix.curator.framework.recipes.cache.PathChildrenCache;
import com.netflix.curator.utils.ThreadUtils;
import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

public class CuratorGlobalSync implements Closeable
{
    private final List<InternalSyncSpec> specs;
    private final NodeSyncQueue nodeSyncQueue;
    private final AtomicReference<State> state = new AtomicReference<State>(State.LATENT);

    private enum State
    {
        LATENT,
        STARTED,
        CLOSED
    }

    public CuratorGlobalSync(CuratorFramework localClient, Collection<SyncSpec> specs, CuratorFramework... remoteClients)
    {
        this(localClient, specs, makeExecutorService(), Arrays.asList(remoteClients));
    }

    public CuratorGlobalSync(CuratorFramework localClient, Collection<SyncSpec> specs, Collection<CuratorFramework> remoteClients)
    {
        this(localClient, specs, makeExecutorService(), remoteClients);
    }

    public CuratorGlobalSync(CuratorFramework localClient, Collection<SyncSpec> specs, ExecutorService service, CuratorFramework... remoteClients)
    {
        this(localClient, specs, service, Arrays.asList(remoteClients));
    }

    public CuratorGlobalSync(CuratorFramework localClient, Collection<SyncSpec> specs, ExecutorService service, Collection<CuratorFramework> remoteClients)
    {
        remoteClients = Preconditions.checkNotNull(remoteClients, "remoteClients cannot be null");
        specs = Preconditions.checkNotNull(specs, "specs cannot be null");
        localClient = Preconditions.checkNotNull(localClient, "localClient cannot be null");
        Preconditions.checkArgument(!remoteClients.contains(localClient), "The remoteClients list cannot contain the localClient");

        ImmutableList.Builder<InternalSyncSpec> builder = ImmutableList.builder();
        for ( SyncSpec spec : specs )
        {
            InternalSyncSpec internalSyncSpec = makeInternalSyncSpec(localClient, service, spec);
            builder.add(internalSyncSpec);
        }
        this.specs = builder.build();

        nodeSyncQueue = new NodeSyncQueue(this, service, remoteClients);
    }

    public void postNodeToSync(ChildData nodeData)
    {
        nodeSyncQueue.add(nodeData);
    }

    public void start() throws Exception
    {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "Already started");

        nodeSyncQueue.start();
        for ( InternalSyncSpec spec : specs )
        {
            spec.start();
        }
    }

    @Override
    public void close() throws IOException
    {
        if ( state.compareAndSet(State.STARTED, State.CLOSED) )
        {
            for ( InternalSyncSpec spec : specs )
            {
                Closeables.closeQuietly(spec);
            }
        }
        Closeables.closeQuietly(nodeSyncQueue);
    }

    private InternalSyncSpec makeInternalSyncSpec(CuratorFramework localClient, ExecutorService service, SyncSpec spec)
    {
        InternalSyncSpec internalSyncSpec;
        switch ( spec.getType() )
        {
            case SINGLE_NODE:
            {
                internalSyncSpec = new InternalSyncSpec(this, new NodeCache(localClient, spec.getPath(), false));
                break;
            }

            case SINGLE_NODE_COMPRESSED_DATA:
            {
                internalSyncSpec = new InternalSyncSpec(this, new NodeCache(localClient, spec.getPath(), true));
                break;
            }

            case CHILDREN_OF_NODE:
            {
                internalSyncSpec = new InternalSyncSpec(this, new PathChildrenCache(localClient, spec.getPath(), true, false, service));
                break;
            }

            case CHILDREN_OF_NODE_COMPRESSED_DATA:
            {
                internalSyncSpec = new InternalSyncSpec(this, new PathChildrenCache(localClient, spec.getPath(), true, true, service));
                break;
            }

            default:
            {
                throw new RuntimeException();   // will never get here
            }
        }
        return internalSyncSpec;
    }

    private static ExecutorService makeExecutorService()
    {
        return Executors.newCachedThreadPool(ThreadUtils.newThreadFactory("CuratorGlobalSync"));
    }
}
