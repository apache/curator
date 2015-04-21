package org.apache.curator.x.discovery.details;

import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.x.discovery.ServiceInstance;
import java.util.concurrent.locks.ReentrantLock;

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
    private final ReentrantLock lock = new ReentrantLock();

    Holder(ServiceInstance<T> service)
    {
        this.service = service;
        setState(State.NEW);
    }

    ServiceInstance<T> getService()
    {
        lock.lock();
        try
        {
            return service;
        }
        finally
        {
            lock.unlock();
        }
    }

    ServiceInstance<T> getServiceIfRegistered()
    {
        lock.lock();
        try
        {
            return (state == State.REGISTERED) ? service : null;
        }
        finally
        {
            lock.unlock();
        }
    }

    void setService(ServiceInstance<T> service)
    {
        lock.lock();
        try
        {
            this.service = service;
        }
        finally
        {
            lock.unlock();
        }
    }

    NodeCache getCache()
    {
        lock.lock();
        try
        {
            return cache;
        }
        finally
        {
            lock.unlock();
        }
    }

    NodeCache getAndClearCache()
    {
        lock.lock();
        try
        {
            NodeCache localCache = cache;
            cache = null;
            return localCache;
        }
        finally
        {
            lock.unlock();
        }
    }

    void setCache(NodeCache cache)
    {
        lock.lock();
        try
        {
            this.cache = cache;
        }
        finally
        {
            lock.unlock();
        }
    }

    State getState()
    {
        lock.lock();
        try
        {
            return state;
        }
        finally
        {
            lock.unlock();
        }
    }

    boolean isRegistered()
    {
        lock.lock();
        try
        {
            return state == State.REGISTERED;
        }
        finally
        {
            lock.unlock();
        }
    }

    boolean isLapsedUnregistered(int cleanThresholdMs)
    {
        lock.lock();
        try
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
        finally
        {
            lock.unlock();
        }
    }

    void setState(State state)
    {
        lock.lock();
        try
        {
            this.state = state;
            stateChangeMs = System.currentTimeMillis();
        }
        finally
        {
            lock.unlock();
        }
    }

    ReentrantLock getLock()
    {
        return lock;
    }
}
