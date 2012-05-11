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

import com.google.common.base.Preconditions;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.listen.ListenerContainer;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * <p>An implementation of the Distributed Priority Queue ZK recipe.</p>
 * <p>Internally, this uses a {@link DistributedQueue}. The only difference is that you specify a
 * priority when putting into the queue.</p>
 * <p>IMPORTANT NOTE: The priority queue will perform far worse than a standard queue. Every time an
 * item is added to/removed from the queue, every watcher must re-get all the nodes</p>
s */
public class DistributedPriorityQueue<T> implements Closeable, QueueBase<T>
{
    private final DistributedQueue<T>      queue;

    DistributedPriorityQueue
        (
            CuratorFramework client,
            QueueConsumer<T> consumer,
            QueueSerializer<T> serializer,
            String queuePath,
            ThreadFactory threadFactory,
            Executor executor,
            int minItemsBeforeRefresh,
            String lockPath)
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
        );
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
     * @param priority item's priority - lower numbers come out of the queue first
     * @throws Exception connection issues
     */
    public void     put(T item, int priority) throws Exception
    {
        queue.checkState();

        String      priorityHex = priorityToString(priority);
        queue.internalPut(item, null, queue.makeItemPath() + priorityHex);
    }

    /**
     * Add a set of items with the same priority into the queue. Adding is done in the background - thus, this method will
     * return quickly.
     *
     * @param items items to add
     * @param priority item priority - lower numbers come out of the queue first
     * @throws Exception connection issues
     */
    public void     putMulti(MultiItem<T> items, int priority) throws Exception
    {
        queue.checkState();

        String      priorityHex = priorityToString(priority);
        queue.internalPut(null, items, queue.makeItemPath() + priorityHex);
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

    /**
     * The default method of converting a priority into a sortable string
     *
     * @param priority the priority
     * @return string
     */
    public static String defaultPriorityToString(int priority)
    {
        // the padded hex val of the number prefixed with a 0 for negative numbers
        // and a 1 for positive (so that it sorts correctly)
        long        l = (long)priority & 0xFFFFFFFFL;
        return String.format("%s%08X", (priority >= 0) ? "1" : "0", l);
    }

    protected String priorityToString(int priority)
    {
        return defaultPriorityToString(priority);
    }
}
