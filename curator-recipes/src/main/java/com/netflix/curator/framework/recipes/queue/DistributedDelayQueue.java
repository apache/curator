/*
 *
 *  Copyright 2011 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.curator.framework.recipes.queue;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.listen.ListenerContainer;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *     A variation of the DistributedPriorityQueue that uses time as the priority. When items
 *     are added to the queue, a delay value is given. The item will not be sent to a consumer
 *     until the time elapses.
 * </p>
 */
public class DistributedDelayQueue<T> implements Closeable, QueueBase<T>
{
    private final DistributedQueue<T>      queue;

    private static final String            SEPARATOR = "|";

    DistributedDelayQueue
        (
            CuratorFramework client,
            QueueConsumer<T> consumer,
            QueueSerializer<T> serializer,
            String queuePath,
            ThreadFactory threadFactory,
            Executor executor,
            int minItemsBeforeRefresh,
            String lockPath
        )
    {
        Preconditions.checkArgument(minItemsBeforeRefresh >= 0, "minItemsBeforeRefresh cannot be negative");

        queue = new DistributedQueue<T>
        (
            client,
            consumer, 
            serializer,
            queuePath,
            threadFactory,
            executor,
            minItemsBeforeRefresh,
            true,
            lockPath
        )
        {
            protected long getDelay(List<String> children)
            {
                if ( children.size() > 0 )
                {
                    long epoch = getEpoch(children.get(0));
                    return epoch - System.currentTimeMillis();
                }
                return 0;
            }
        };
    }

    /**
     * Start the queue. No other methods work until this is called
     *
     * @throws Exception startup errors
     */
    @Override
    public void     start() throws Exception
    {
        queue.start();
    }

    @Override
    public void close() throws IOException
    {
        queue.close();
    }

    /**
     * Add an item into the queue. Adding is done in the background - thus, this method will
     * return quickly.
     *
     * @param item item to add
     * @param delayUntilEpoch future epoch (milliseconds) when this item will be available to consumers
     * @throws Exception connection issues
     */
    public void     put(T item, long delayUntilEpoch) throws Exception
    {
        Preconditions.checkArgument(delayUntilEpoch > 0, "delayUntilEpoch cannot be negative");

        queue.checkState();

        queue.internalPut(item, null, queue.makeItemPath() + epochToString(delayUntilEpoch));
    }

    /**
     * Add a set of items with the same priority into the queue. Adding is done in the background - thus, this method will
     * return quickly.
     *
     * @param items items to add
     * @param delayUntilEpoch future epoch (milliseconds) when this item will be available to consumers
     * @throws Exception connection issues
     */
    public void     putMulti(MultiItem<T> items, long delayUntilEpoch) throws Exception
    {
        Preconditions.checkArgument(delayUntilEpoch > 0, "delayUntilEpoch cannot be negative");

        queue.checkState();

        queue.internalPut(null, items, queue.makeItemPath() + epochToString(delayUntilEpoch));
    }

    @Override
    public void setErrorMode(ErrorMode newErrorMode)
    {
        queue.setErrorMode(newErrorMode);
    }

    @Override
    public boolean flushPuts(long waitTime, TimeUnit timeUnit) throws InterruptedException
    {
        return queue.flushPuts(waitTime, timeUnit);
    }

    /**
     * Return the manager for put listeners
     *
     * @return put listener container
     */
    @Override
    public ListenerContainer<QueuePutListener<T>> getPutListenerContainer()
    {
        return queue.getPutListenerContainer();
    }

    /**
     * Return the most recent message count from the queue. This is useful for debugging/information
     * purposes only.
     *
     * @return count (can be 0)
     */
    @Override
    public int getLastMessageCount()
    {
        return queue.getLastMessageCount();
    }

    @VisibleForTesting
    static String epochToString(long epoch)
    {
        return SEPARATOR + String.format("%08X", epoch) + SEPARATOR;
    }

    private long getEpoch(String itemNode)
    {
        int     index2 = itemNode.lastIndexOf(SEPARATOR);
        int     index1 = (index2 > 0) ? itemNode.lastIndexOf(SEPARATOR, index2 - 1) : -1;
        if ( (index1 > 0) && (index2 > (index1 + 1)) )
        {
            try
            {
                String  epochStr = itemNode.substring(index1 + 1, index2);
                return Long.parseLong(epochStr, 16);
            }
            catch ( NumberFormatException ignore )
            {
                // ignore
            }
        }
        return 0;
    }
}
