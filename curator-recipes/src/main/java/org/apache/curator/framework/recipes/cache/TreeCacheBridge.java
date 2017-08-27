package org.apache.curator.framework.recipes.cache;

import org.apache.curator.framework.listen.Listenable;
import java.io.Closeable;
import java.util.Map;

public interface TreeCacheBridge extends Closeable
{
    /**
     * Start the cache. The cache is not started automatically. You must call this method.
     *
     * @return this
     * @throws Exception errors
     */
    TreeCacheBridge start() throws Exception;

    /**
     * Close/end the cache.
     */
    @Override
    void close();

    /**
     * Return the cache listenable
     *
     * @return listenable
     */
    Listenable<TreeCacheListener> getListenable();

    /**
     * Return the current set of children at the given path, mapped by child name. There are no
     * guarantees of accuracy; this is merely the most recent view of the data.  If there is no
     * node at this path, an empty list or {@code null} is returned (depending on implementation).
     *
     * @param fullPath full path to the node to check
     * @return a possibly-empty list of children if the node is alive, or null (depending on implementation)
     */
    Map<String, ChildData> getCurrentChildren(String fullPath);

    /**
     * Return the current data for the given path. There are no guarantees of accuracy. This is
     * merely the most recent view of the data. If there is no node at the given path,
     * {@code null} is returned.
     *
     * @param fullPath full path to the node to check
     * @return data if the node is alive, or null
     */
    ChildData getCurrentData(String fullPath);
}
