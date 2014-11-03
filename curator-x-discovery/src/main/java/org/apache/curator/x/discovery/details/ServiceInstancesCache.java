package org.apache.curator.x.discovery.details;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.x.discovery.ServiceInstance;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ThreadFactory;

class ServiceInstancesCache<T> extends AbstractNodeCache<T> implements Closeable
{

    private final ServiceCacheImpl<T> serviceCache;

    private final Function<ServiceCacheListener, Void> cacheChangedFunction =  new Function<ServiceCacheListener, Void>()
    {
        @Override
        public Void apply(ServiceCacheListener listener)
        {
            listener.cacheChanged();
            return null;
        }
    };

    ServiceInstancesCache(ServiceDiscoveryImpl<T> discovery, String name, ThreadFactory threadFactory, ServiceCacheImpl<T> serviceCache)
    {
        super(discovery, discovery.pathForName(name), threadFactory);
        this.serviceCache = serviceCache;

    }

    public List<ServiceInstance<T>> getInstances()
    {
        return Lists.newArrayList(instances.values());
    }

    @Override
    public void close() throws IOException
    {
        CloseableUtils.closeQuietly(cache);
    }

    @Override
    void onChildAdded(PathChildrenCacheEvent event) throws Exception
    {
        onChildUpdated(event);
    }

    @Override
    void onChildUpdated(PathChildrenCacheEvent event) throws Exception
    {
        addInstance(event.getData(), false);

        serviceCache.forEach(cacheChangedFunction);

    }

    @Override
    void onChildRemoved(PathChildrenCacheEvent event)
    {
        instances.remove(instanceIdFromData(event.getData()));

        serviceCache.forEach(cacheChangedFunction);
    }
}