package org.apache.curator.framework.api;

import org.apache.zookeeper.Watcher;

/**
 * Builder to allow watches to be removed 
 */
public interface RemoveWatchesBuilder
{
    /**
     * Specify the watcher to be removed
     * @param watcher
     * @return
     */
    public RemoveWatchesType watcher(Watcher watcher);
    
    /**
     * Specify the watcher to be removed
     * @param watcher
     * @return
     */
    public RemoveWatchesType watcher(CuratorWatcher watcher);
    
    /**
     * Specify that all watches should be removed
     * @return
     */
    public RemoveWatchesType allWatches();
}
