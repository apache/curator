/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.curator.framework.recipes.queue;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.listen.Listenable;
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
 */
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
            String lockPath,
            int maxItems,
            boolean putInBackground,
            int finalFlushMs
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
            lockPath,
            maxItems,
            putInBackground,
            finalFlushMs
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
     * return quickly.<br><br>
     * NOTE: if an upper bound was set via {@link QueueBuilder#maxItems}, this method will
     * block until there is available space in the queue.
     *
     * @param item item to add
     * @param priority item's priority - lower numbers come out of the queue first
     * @throws Exception connection issues
     */
    public void     put(T item, int priority) throws Exception
    {
        put(item, priority, 0, null);
    }

    /**
     * Same as {@link #put(Object, int)} but allows a maximum wait time if an upper bound was set
     * via {@link QueueBuilder#maxItems}.
     *
     * @param item item to add
     * @param priority item's priority - lower numbers come out of the queue first
     * @param maxWait maximum wait
     * @param unit wait unit
     * @return true if items was added, false if timed out
     * @throws Exception
     */
    public boolean     put(T item, int priority, int maxWait, TimeUnit unit) throws Exception
    {
        queue.checkState();

        String      priorityHex = priorityToString(priority);
        return queue.internalPut(item, null, queue.makeItemPath() + priorityHex, maxWait, unit);
    }

    /**
     * Add a set of items with the same priority into the queue. Adding is done in the background - thus, this method will
     * return quickly.<br><br>
     * NOTE: if an upper bound was set via {@link QueueBuilder#maxItems}, this method will
     * block until there is available space in the queue.
     *
     * @param items items to add
     * @param priority item priority - lower numbers come out of the queue first
     * @throws Exception connection issues
     */
    public void     putMulti(MultiItem<T> items, int priority) throws Exception
    {
        queue.putMulti(items, priority, priorityToString(priority), null);
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
    public Listenable<QueuePutListener<T>> getPutListenerContainer()
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
    ChildrenCache getCache()
    {
        return queue.getCache();
    }

    @VisibleForTesting
    static String priorityToString(int priority)
    {
        // the padded hex val of the number prefixed with a 0 for negative numbers
        // and a 1 for positive (so that it sorts correctly)
        long        l = (long)priority & 0xFFFFFFFFL;
        return String.format("%s%08X", (priority >= 0) ? "1" : "0", l);
    }
}
