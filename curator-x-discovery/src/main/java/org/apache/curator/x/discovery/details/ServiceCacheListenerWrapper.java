package org.apache.curator.x.discovery.details;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.x.discovery.ServiceInstance;

class ServiceCacheListenerWrapper<T> implements ServiceCacheEventListener<T>
{
    private final ServiceCacheListener listener;

    static <T> ServiceCacheListenerWrapper<T> wrap(ServiceCacheListener listener)
    {
        return new ServiceCacheListenerWrapper<>(listener);
    }

    static ServiceCacheListener unwrap(ServiceCacheEventListener<?> eventListener)
    {
        if ( eventListener instanceof ServiceCacheListenerWrapper )
        {
            return ((ServiceCacheListenerWrapper)eventListener).listener;
        }
        return null;
    }

    private ServiceCacheListenerWrapper(ServiceCacheListener listener)
    {
        this.listener = listener;
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState)
    {
        listener.stateChanged(client, newState);
    }

    @Override
    public void cacheAdded(ServiceInstance<T> added)
    {
        listener.cacheChanged();
    }

    @Override
    public void cacheDeleted(ServiceInstance<T> deleted)
    {
        listener.cacheChanged();
    }

    @Override
    public void cacheUpdated(ServiceInstance<T> old, ServiceInstance<T> updated)
    {
        listener.cacheChanged();
    }
}
