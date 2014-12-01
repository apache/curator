package org.apache.curator.framework.api;

import org.apache.zookeeper.Watcher.WatcherType;

/**
 * Builder to allow the specification of whether it is acceptable to remove client side watch information
 * in the case where ZK cannot be contacted. 
 */
public interface RemoveWatchesType
{
   
    /**
     * Specify the type of watcher to be removed.
     * @param watcherType
     * @return
     */
    public RemoveWatchesLocal ofType(WatcherType watcherType);
    
}
