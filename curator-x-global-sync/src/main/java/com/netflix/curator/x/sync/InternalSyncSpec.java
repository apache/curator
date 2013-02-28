package com.netflix.curator.x.sync;

import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.cache.NodeCache;
import com.netflix.curator.framework.recipes.cache.NodeCacheListener;
import com.netflix.curator.framework.recipes.cache.PathChildrenCache;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheEvent;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheListener;
import java.io.Closeable;

class InternalSyncSpec implements Closeable, PathChildrenCacheListener, NodeCacheListener
{
    private final PathChildrenCache pathCache;
    private final NodeCache nodeCache;
    private final CuratorGlobalSync sync;

    InternalSyncSpec(CuratorGlobalSync sync, PathChildrenCache pathCache)
    {
        this.sync = sync;
        this.pathCache = pathCache;
        nodeCache = null;

        pathCache.getListenable().addListener(this);
    }

    InternalSyncSpec(CuratorGlobalSync sync, NodeCache nodeCache)
    {
        this.sync = sync;
        pathCache = null;
        this.nodeCache = nodeCache;

        nodeCache.getListenable().addListener(this);
    }

    public void start() throws Exception
    {
        if ( pathCache != null )
        {
            pathCache.start();
        }

        if ( nodeCache != null )
        {
            nodeCache.start();
        }
    }

    @Override
    public void close()
    {
        Closeables.closeQuietly(pathCache);
        Closeables.closeQuietly(nodeCache);
    }

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
    {
        // TODO what about deletes?
        sync.postNodeToSync(event.getData());
    }

    @Override
    public void nodeChanged() throws Exception
    {
        // TODO what about deletes?
        sync.postNodeToSync(nodeCache.getCurrentData());
    }
}
