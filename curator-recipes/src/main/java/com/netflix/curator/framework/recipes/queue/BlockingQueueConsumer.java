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

import com.google.common.collect.ImmutableList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Utility - a queue consumer that provides behavior similar to a {@link BlockingQueue}
 */
public class BlockingQueueConsumer<T> implements QueueConsumer<T>
{
    private final BlockingQueue<T>      items;

    /**
     * Creates with capacity of {@link Integer#MAX_VALUE}
     */
    public BlockingQueueConsumer()
    {
        items = new LinkedBlockingQueue<T>();
    }

    /**
     * @param capacity max capacity (i.e. puts block if full)
     */
    public BlockingQueueConsumer(int capacity)
    {
        items = new ArrayBlockingQueue<T>(capacity);
    }

    @Override
    public void consumeMessage(T message) throws Exception
    {
        items.add(message);
    }

    /**
     * Return any currently queued items without removing them from the queue
     *
     * @return items (can be empty)
     */
    public List<T> getItems()
    {
        return ImmutableList.copyOf(items);
    }

    /**
     * Returns the number of currently queue items
     *
     * @return currently queue item count or 0
     */
    public int  size()
    {
        return items.size();
    }

    /**
     * Take the next item from the queue, blocking until there is an item available
     *
     * @return the item
     * @throws InterruptedException thread interruption
     */
    public T take() throws InterruptedException
    {
        return items.take();
    }

    /**
     * Take the next item from the queue, waiting up to the specified time for
     * an available item. If the time elapses, <code>null</code> is returned.
     *
     * @param time amount of time to block
     * @param unit time unit
     * @return next item or null
     * @throws InterruptedException thread interruption
     */
    public T take(int time, TimeUnit unit) throws InterruptedException
    {
        return items.poll(time, unit);
    }

    /**
     * Removes all available elements from this queue and adds them
     * to the given collection.  This operation may be more
     * efficient than repeatedly polling this queue.  A failure
     * encountered while attempting to add elements to
     * collection <tt>c</tt> may result in elements being in neither,
     * either or both collections when the associated exception is
     * thrown.  Attempts to drain a queue to itself result in
     * <tt>IllegalArgumentException</tt>. Further, the behavior of
     * this operation is undefined if the specified collection is
     * modified while the operation is in progress.
     *
     * @param c the collection to transfer elements into
     * @return the number of elements transferred
     * @throws UnsupportedOperationException if addition of elements
     *         is not supported by the specified collection
     * @throws ClassCastException if the class of an element of this queue
     *         prevents it from being added to the specified collection
     * @throws NullPointerException if the specified collection is null
     * @throws IllegalArgumentException if the specified collection is this
     *         queue, or some property of an element of this queue prevents
     *         it from being added to the specified collection
     */
    public int drainTo(Collection<? super T> c)
    {
        return items.drainTo(c);
    }
}
