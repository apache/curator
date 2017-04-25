package org.apache.curator.x.async.modeled.caching;

import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.x.async.modeled.recipes.ModeledCacheListener;
import java.io.Closeable;

public interface Caching<T> extends Closeable
{
    /**
     * Forwards to the internal cache's start method. Not idempotent.
     */
    void start();

    /**
     * Forwards to the internal cache's close method.
     */
    @Override
    void close();

    /**
     * Return the listener container so that you can add/remove listeners
     *
     * @return listener container
     */
    Listenable<ModeledCacheListener<T>> getListenable();
}
