package org.apache.curator.x.discovery.details;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;
import org.apache.curator.x.discovery.ServiceInstance;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadFactory;

abstract class AbstractNodeCache<T> implements PathChildrenCacheListener {

    protected final PathChildrenCache cache;
    protected final ServiceDiscoveryImpl<T> discovery;
    protected final ConcurrentMap<String, ServiceInstance<T>> instances = Maps.newConcurrentMap();
    protected final ThreadFactory threadFactory;

    protected AbstractNodeCache(ServiceDiscoveryImpl<T> discovery, String name, ThreadFactory threadFactory)
    {
        Preconditions.checkNotNull(discovery, "discovery cannot be null");
        Preconditions.checkNotNull(name, "name cannot be null");
        Preconditions.checkNotNull(threadFactory, "threadFactory cannot be null");

        this.discovery = discovery;
        this.threadFactory = threadFactory;

        cache = new PathChildrenCache(discovery.getClient(), name, true, threadFactory);
        cache.getListenable().addListener(this);

    }

    abstract void onChildAdded(PathChildrenCacheEvent event) throws Exception;

    abstract void onChildUpdated(PathChildrenCacheEvent event) throws Exception;

    abstract void onChildRemoved(PathChildrenCacheEvent event) throws Exception;

    protected void start() throws Exception
    {
        cache.start(true);

        for ( ChildData childData : cache.getCurrentData() )
        {
            addInstance(childData, true);
        }
    }

    protected String instanceIdFromData(ChildData childData)
    {
        return ZKPaths.getNodeFromPath(childData.getPath());
    }

    protected void addInstance(ChildData childData, boolean onlyIfAbsent) throws Exception
    {
        String instanceId = instanceIdFromData(childData);
        ServiceInstance<T> serviceInstance = discovery.getSerializer().deserialize(childData.getData());
        if (onlyIfAbsent)
        {
            instances.putIfAbsent(instanceId, serviceInstance);
        } else
        {
            instances.put(instanceId, serviceInstance);
        }
        cache.clearDataBytes(childData.getPath(), childData.getStat().getVersion());
    }


    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
        switch (event.getType())
        {
            case CHILD_ADDED:
            {
                onChildAdded(event);
                break;
            }
            case CHILD_UPDATED:
            {
                onChildUpdated(event);
                break;
            }

            case CHILD_REMOVED:
            {
                onChildRemoved(event);
                break;
            }
        }
    }
}
