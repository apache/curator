package org.apache.curator.framework.recipes.cache;

public interface PersistentWatcherCacheListener
{
    void nodeCreated(String path);

    void nodeDeleted(String path);

    void nodeDataChanged(String path);

    void nodeDataAvailable(String path);
}
