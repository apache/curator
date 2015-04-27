package org.apache.curator.x.discovery.details;

import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.x.discovery.ServiceInstance;

class Holder<T>
{
    enum State
    {
        NEW,
        REGISTERED,
        UNREGISTERED
    }

    private ServiceInstance<T> service;
    private NodeCache cache;
    private State state;
    private long stateChangeMs;

    Holder(ServiceInstance<T> service, NodeCache nodeCache)
    {
        cache = nodeCache;
        this.service = service;
        setState(State.NEW);
    }

    synchronized ServiceInstance<T> getService()
    {
        return service;
    }

    synchronized ServiceInstance<T> getServiceIfRegistered()
    {
        return (state == State.REGISTERED) ? service : null;
    }

    synchronized void setService(ServiceInstance<T> service)
    {
        this.service = service;
    }

    synchronized NodeCache getAndClearCache()
    {
        NodeCache localCache = cache;
        cache = null;
        return localCache;
    }

    synchronized boolean isRegistered()
    {
        return state == State.REGISTERED;
    }

    synchronized boolean isLapsedUnregistered(int cleanThresholdMs)
    {
        if ( state == State.UNREGISTERED )
        {
            long elapsed = System.currentTimeMillis() - stateChangeMs;
            if ( elapsed >= cleanThresholdMs )
            {
                return true;
            }
        }
        return false;
    }

    synchronized void setState(State state)
    {
        this.state = state;
        stateChangeMs = System.currentTimeMillis();
    }
}
