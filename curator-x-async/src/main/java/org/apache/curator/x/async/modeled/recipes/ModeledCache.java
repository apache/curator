package org.apache.curator.x.async.modeled.recipes;

import org.apache.curator.x.async.modeled.ZPath;
import java.util.Optional;

public interface ModeledCache<T>
{
    /**
     * Return the modeled current data for the given path. There are no guarantees of accuracy. This is
     * merely the most recent view of the data. If there is no node at the given path,
     * {@link java.util.Optional#empty()} is returned.
     *
     * @param fullPath full path to the node to check
     * @return data if the node is alive, or null
     */
    Optional<ModeledCachedNode<T>> getCurrentData(ZPath fullPath);
}
