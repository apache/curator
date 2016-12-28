package org.apache.curator.framework.recipes.cache;

public interface PersistentWatcherCacheFilter
{
    enum Action
    {
        IGNORE,
        SAVE_ONLY,
        SAVE_THEN_GET_DATA,
        GET_DATA_THEN_SAVE
    }

    Action actionForPath(String path);
}
