/**
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
import org.apache.curator.framework.listen.ListenerContainer;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
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
        )
        {
            protected long getDelay(String itemNode)
            {
                long epoch = getEpoch(itemNode);
                return epoch - System.currentTimeMillis();
            }

            protected void sortChildren(List<String> children)
            {
                Collections.sort
                (
                    children,
                    new Comparator<String>()
                    {
                        @Override
                        public int compare(String o1, String o2)
                        {
                            long        diff = getDelay(o1) - getDelay(o2);
                            return (diff < 0) ? -1 : ((diff > 0) ? 1 : 0);
                        }
                    }
                );
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
     * return quickly.<br><br>
     * NOTE: if an upper bound was set via {@link QueueBuilder#maxItems}, this method will
     * block until there is available space in the queue.
     *
     * @param item item to add
     * @param delayUntilEpoch future epoch (milliseconds) when this item will be available to consumers
     * @throws Exception connection issues
     */
    public void     put(T item, long delayUntilEpoch) throws Exception
    {
        put(item, delayUntilEpoch, 0, null);
    }

    /**
     * Same as {@link #put(Object, long)} but allows a maximum wait time if an upper bound was set
     * via {@link QueueBuilder#maxItems}.
     *
     * @param item item to add
     * @param delayUntilEpoch future epoch (milliseconds) when this item will be available to consumers
     * @param maxWait maximum wait
     * @param unit wait unit
     * @return true if items was added, false if timed out
     * @throws Exception
     */
    public boolean      put(T item, long delayUntilEpoch, int maxWait, TimeUnit unit) throws Exception
    {
        Preconditions.checkArgument(delayUntilEpoch > 0, "delayUntilEpoch cannot be negative");

        queue.checkState();

        return queue.internalPut(item, null, queue.makeItemPath() + epochToString(delayUntilEpoch), maxWait, unit);
    }

    /**
     * Add a set of items with the same priority into the queue. Adding is done in the background - thus, this method will
     * return quickly.<br><br>
     * NOTE: if an upper bound was set via {@link QueueBuilder#maxItems}, this method will
     * block until there is available space in the queue.
     *
     * @param items items to add
     * @param delayUntilEpoch future epoch (milliseconds) when this item will be available to consumers
     * @throws Exception connection issues
     */
    public void     putMulti(MultiItem<T> items, long delayUntilEpoch) throws Exception
    {
        putMulti(items, delayUntilEpoch, 0, null);
    }

    /**
     * Same as {@link #putMulti(MultiItem, long)} but allows a maximum wait time if an upper bound was set
     * via {@link QueueBuilder#maxItems}.
     *
     * @param items items to add
     * @param delayUntilEpoch future epoch (milliseconds) when this item will be available to consumers
     * @param maxWait maximum wait
     * @param unit wait unit
     * @return true if items was added, false if timed out
     * @throws Exception
     */
    public boolean      putMulti(MultiItem<T> items, long delayUntilEpoch, int maxWait, TimeUnit unit) throws Exception
    {
        Preconditions.checkArgument(delayUntilEpoch > 0, "delayUntilEpoch cannot be negative");

        queue.checkState();

        return queue.internalPut(null, items, queue.makeItemPath() + epochToString(delayUntilEpoch), maxWait, unit);
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

    private static long getEpoch(String itemNode)
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
