package com.netflix.curator.framework.recipes.queue;

/**
 * Abstraction for multiple items.
 * @see DistributedQueue#putMulti(MultiItem)
 * @see DistributedPriorityQueue#putMulti(MultiItem, int)
 * @param <T> queue item type
 */
public interface MultiItem<T>
{
    /**
     * Called repeatedly to get the items to add to the queue. This method
     * should return <code>null</code> when there are no more items to add.
     *
     * @return item or null
     * @throws Exception any errors
     */
    public T    nextItem() throws Exception;
}
