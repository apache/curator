package org.apache.curator.framework.recipes.watch;

public class CacheFilters
{
    private static final CacheFilter statAndData = new StandardCacheFilter(CacheAction.STAT_AND_DATA);
    private static final CacheFilter compressedStatAndData = new StandardCacheFilter(CacheAction.STAT_AND_COMPRESSED_DATA);
    private static final CacheFilter statOnly = new StandardCacheFilter(CacheAction.STAT_ONLY);
    private static final CacheFilter pathOnly = new StandardCacheFilter(CacheAction.PATH_ONLY);

    public static CacheFilter statAndData()
    {
        return statAndData;
    }

    public static CacheFilter compressedData()
    {
        return compressedStatAndData;
    }

    public static CacheFilter statOnly()
    {
        return statOnly;
    }

    public static CacheFilter pathOnly()
    {
        return pathOnly;
    }

    public static CacheFilter full(final CacheAction cacheAction)
    {
        return new CacheFilter()
        {
            @Override
            public CacheAction actionForPath(String mainPath, String checkPath)
            {
                return cacheAction;
            }
        };
    }

    private CacheFilters()
    {
    }
}
