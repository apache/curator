package org.apache.curator.x.async.modeled;

import org.apache.curator.x.async.modeled.recipes.ModeledCache;
import java.io.Closeable;

public interface CachedModeledCuratorFramework<T> extends ModeledCuratorFramework<T>, Closeable
{
    /**
     * Return the cache instance
     *
     * @return cache
     */
    ModeledCache<T> getCache();

    /**
     * Start the internally created via {@link #cached()}
     */
    void start();

    /**
     * Close the internally created via {@link #cached()}
     */
    @Override
    void close();
}
