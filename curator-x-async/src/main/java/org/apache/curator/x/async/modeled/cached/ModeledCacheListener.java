package org.apache.curator.x.async.modeled.cached;

import org.apache.curator.x.async.modeled.ZPath;
import org.apache.zookeeper.data.Stat;

@FunctionalInterface
public interface ModeledCacheListener<T>
{
    /**
     * The given path was added, updated or removed
     *
     * @param type action type
     * @param path the path
     * @param stat the node's stat (previous stat for removal)
     * @param model the node's model (previous model for removal)
     */
    void accept(ModeledCacheEventType type, ZPath path, Stat stat, T model);

    /**
     * The cache has finished initializing
     */
    default void initialized()
    {
        // NOP
    }

    /**
     * Returns a version of this listener that only begins calling
     * {@link #accept(ModeledCacheEventType, org.apache.curator.x.async.modeled.ZPath, org.apache.zookeeper.data.Stat, Object)}
     * once {@link #initialized()} has been called. i.e. changes that occur as the cache is initializing are not sent
     * to the listener
     *
     * @return wrapped listener
     */
    default ModeledCacheListener<T> postInitializedOnly()
    {
        return new ModeledCacheListener<T>()
        {
            private volatile boolean isInitialized = false;

            @Override
            public void accept(ModeledCacheEventType type, ZPath path, Stat stat, T model)
            {
                if ( isInitialized )
                {
                    ModeledCacheListener.this.accept(type, path, stat, model);
                }
            }

            @Override
            public void initialized()
            {
                isInitialized = true;
                ModeledCacheListener.this.initialized();
            }
        };
    }
}
