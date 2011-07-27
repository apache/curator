package com.netflix.curator.framework.recipes.cache;

/**
 * Controls which data is cached
 */
public enum PathChildrenCacheMode
{
    /**
     * The cache will hold all the children, the data for each child node
     * and the stat for each child node
     */
    CACHE_DATA_AND_STAT,

    /**
     * The cache will hold all the children and the data for each child node.
     * {@link ChildData#getStat()} will return <code>null</code>.
     */
    CACHE_DATA,

    /**
     * The cache will hold only the children path names.
     * {@link ChildData#getStat()} and {@link ChildData#getData()} will both return <code>null</code>.
     */
    CACHE_PATHS_ONLY
}
