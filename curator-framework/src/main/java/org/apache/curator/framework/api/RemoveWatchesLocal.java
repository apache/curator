package org.apache.curator.framework.api;

/**
 * Builder to allow the specification of whether it is acceptable to remove client side watch information
 * in the case where ZK cannot be contacted. 
 */
public interface RemoveWatchesLocal extends BackgroundPathable<Void>
{
   
    /**
     * Specify if the client should just remove client side watches if a connection to ZK
     * is not available.
     * @param local
     * @return
     */
    public BackgroundPathable<Void> local(boolean local);
    
}
