/*
 Copyright 2011 Netflix, Inc.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
 */
package com.netflix.curator.framework.recipes.queue;

import com.google.common.base.Preconditions;
import com.netflix.curator.framework.CuratorFramework;
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
public class DistributedPriorityQueue<T> implements Closeable
{
    private final DistributedQueue<T>      queue;

    DistributedPriorityQueue
        (
            CuratorFramework    client,
            QueueSerializer<T>  serializer,
            String              queuePath,
            ThreadFactory       threadFactory,
            Executor            executor,
            int                 maxInternalQueue,
            int                 minItemsBeforeRefresh
        )
    {
        Preconditions.checkArgument(minItemsBeforeRefresh >= 0);

        queue = new DistributedQueue<T>
        (
            client,
            serializer,
            queuePath,
            threadFactory,
            executor,
            maxInternalQueue,
            minItemsBeforeRefresh,
            true
        );
    }

    /**
     * Start the queue. No other methods work until this is called
     *
     * @throws Exception startup errors
     */
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

    /**
     * Take the next item off of the queue blocking until there is an item available
     *
     * @return the item
     * @throws Exception thread interruption or an error in the background thread
     */
    public T        take() throws Exception
    {
        return queue.take();
    }

    /**
     * Take the next item off of the queue blocking until there is an item available
     * or the specified timeout has elapsed
     *
     * @param timeout timeout
     * @param unit unit
     * @return the item or null if timed out
     * @throws Exception thread interruption or an error in the background thread
     */
    public T        take(long timeout, TimeUnit unit) throws Exception
    {
        return queue.take(timeout, unit);
    }

    /**
     * Return the number of pending items in the local Java queue. IMPORTANT: when this method
     * returns a non-zero value, there is no guarantee that a subsequent call to take() will not
     * block. i.e. items can get removed between this method call and others.
     *
     * @return item qty or 0
     * @throws Exception an error in the background thread
     */
    public int      available() throws Exception
    {
        return queue.available();
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
