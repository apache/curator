package org.apache.curator.x.async.modeled.details;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.x.async.api.CreateOption;
import org.apache.curator.x.async.modeled.ModelSerializer;
import org.apache.curator.x.async.modeled.ZPath;
import org.apache.curator.x.async.modeled.caching.Caching;
import org.apache.curator.x.async.modeled.caching.CachingOption;
import org.apache.curator.x.async.modeled.recipes.ModeledCacheEvent;
import org.apache.curator.x.async.modeled.recipes.ModeledCacheListener;
import org.apache.curator.x.async.modeled.recipes.ModeledCachedNode;
import org.apache.curator.x.async.modeled.recipes.ModeledTreeCache;
import org.apache.zookeeper.data.Stat;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

class CachingImpl<T> implements Caching<T>, ModeledCacheListener<T>
{
    private final ModeledTreeCache<T> treeCache;
    private final AtomicLong dirtyZxid = new AtomicLong(-1);
    private final ZPath path;
    private final boolean dirtyReads;

    CachingImpl(CuratorFramework client, ModelSerializer<T> serializer, ZPath path, Set<CachingOption> cachingOptions, Set<CreateOption> createOptions)
    {
        this.path = path;
        TreeCache.Builder builder = TreeCache.newBuilder(client, path.fullPath());
        builder = builder.setCacheData(!cachingOptions.contains(CachingOption.metaDataOnly)).setCreateParentNodes(cachingOptions.contains(CachingOption.createParentNodes));
        if ( ModeledCuratorFrameworkImpl.isCompressed(createOptions) )
        {
            builder = builder.setDataIsCompressed(true);
        }
        TreeCache cache = builder.build();
        treeCache = ModeledTreeCache.wrap(cache, serializer);

        treeCache.getListenable().addListener(this);

        dirtyReads = cachingOptions.contains(CachingOption.dirtyReads);
    }

    CachingImpl<T> at(String child)
    {
        return new CachingImpl<>(treeCache, path.at(child), dirtyReads);
    }

    @Override
    public void start()
    {
        treeCache.start();
    }

    @Override
    public void close()
    {
        treeCache.close();
    }

    @Override
    public Listenable<ModeledCacheListener<T>> getListenable()
    {
        return treeCache.getListenable();
    }

    @Override
    public void event(ModeledCacheEvent<T> event)
    {
        switch ( event.getType() )
        {
            case NODE_ADDED:
            case NODE_UPDATED:
            {
                updateDirtyZxid(event.getNode().getStat().getMzxid());
                break;
            }

            case NODE_REMOVED:
            {
                // TODO
                break;
            }

            case CONNECTION_RECONNECTED:
            {
                dirtyZxid.set(-1);
                break;
            }
        }
    }

    long getCurrentZxid()
    {
        return treeCache.getCurrentData(path).map(cache -> (cache.getStat() != null) ? cache.getStat().getMzxid() : -1).orElse(-1L);
    }

    void markDirty(long zxid)
    {
        if ( !dirtyReads && (zxid >= 0) )
        {
            long currentDirtyZxid = dirtyZxid.get();
            if ( zxid > currentDirtyZxid )
            {
                dirtyZxid.compareAndSet(currentDirtyZxid, zxid);
            }
        }
    }

    ModeledTreeCache<T> getCache()
    {
        return treeCache;
    }

    ModeledCachedNode<T> getCacheIf()
    {
        Optional<ModeledCachedNode<T>> currentData = treeCache.getCurrentData(path);
        return currentData.map(this::getDataWithDirtyCheck).orElse(null);
    }

    private ModeledCachedNode<T> getDataWithDirtyCheck(ModeledCachedNode<T> data)
    {
        Stat stat = data.getStat();
        if ( stat != null )
        {
            if ( stat.getMzxid() > dirtyZxid.get() )
            {
                return data;
            }
        }
        return null;
    }

    private void updateDirtyZxid(long newZxid)
    {
        if ( dirtyReads )
        {
            return;
        }

        long currentDirtyZxid = dirtyZxid.get();
        if ( (currentDirtyZxid >= 0) && (newZxid > currentDirtyZxid) )
        {
            if ( !dirtyZxid.compareAndSet(currentDirtyZxid, -1) )
            {
                updateDirtyZxid(newZxid);
            }
        }
    }

    private CachingImpl(ModeledTreeCache<T> treeCache, ZPath path, boolean dirtyReads)
    {
        this.treeCache = treeCache;
        this.path = path;
        this.dirtyReads = dirtyReads;
    }
}
