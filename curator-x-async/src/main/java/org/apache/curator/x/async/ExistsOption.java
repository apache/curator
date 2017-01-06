package org.apache.curator.x.async;

/**
 * Options to use when checking for ZNode existence
 */
public enum ExistsOption
{
    /**
     * see {@link org.apache.curator.x.async.CreateOption#createParentsIfNeeded}
     */
    createParentsIfNeeded,

    /**
     * see {@link org.apache.curator.x.async.CreateOption#createParentsAsContainers}
     */
    createParentsAsContainers
}
