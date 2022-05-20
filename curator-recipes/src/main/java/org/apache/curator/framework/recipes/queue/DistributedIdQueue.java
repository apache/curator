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
import org.apache.curator.framework.listen.Listenable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * A version of {@link DistributedQueue} that allows IDs to be associated with queue items. Items
 * can then be removed from the queue if needed
 */
public class DistributedIdQueue<T> implements QueueBase<T>
{
    private final Logger                log = LoggerFactory.getLogger(getClass());
    private final DistributedQueue<T>   queue;

    private static final char           SEPARATOR = '|';

    private static class Parts
    {
        final String        id;
        final String        cleaned;

        private Parts(String id, String cleaned)
        {
            this.id = id;
            this.cleaned = cleaned;
        }
    }

    DistributedIdQueue
    (
        CuratorFramework client,
        QueueConsumer<T> consumer, QueueSerializer<T>
        serializer, String
        queuePath, ThreadFactory
        threadFactory, Executor
        executor,
        int minItemsBeforeRefresh,
        boolean refreshOnWatch,
        String lockPath,
        int maxItems,
        boolean putInBackground,
        int finalFlushMs
    )
    {
        queue = new DistributedQueue<T>(client, consumer, serializer, queuePath, threadFactory, executor, minItemsBeforeRefresh, refreshOnWatch, lockPath, maxItems, putInBackground, finalFlushMs)
        {
            @Override
            protected void sortChildren(List<String> children)
            {
                internalSortChildren(children);
            }

            @Override
            protected String makeRequeueItemPath(String itemPath)
            {
                return makeIdPath(parseId(itemPath).id);
            }
        };

        if ( queue.makeItemPath().contains(Character.toString(SEPARATOR)) )
        {
            throw new IllegalStateException("DistributedQueue can't use " + SEPARATOR);
        }
    }

    @Override
    public void start() throws Exception
    {
        queue.start();
    }

    @Override
    public void close() throws IOException
    {
        queue.close();
    }

    @Override
    public Listenable<QueuePutListener<T>> getPutListenerContainer()
    {
        return queue.getPutListenerContainer();
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

    @Override
    public int getLastMessageCount()
    {
        return queue.getLastMessageCount();
    }

    /**
     * Put an item into the queue with the given Id<br><br>
     * NOTE: if an upper bound was set via {@link QueueBuilder#maxItems}, this method will
     * block until there is available space in the queue.
     *
     * @param item item
     * @param itemId item Id
     * @throws Exception errors
     */
    public void put(T item, String itemId) throws Exception
    {
        put(item, itemId, 0, null);
    }

    /**
     * Same as {@link #put(Object, String)} but allows a maximum wait time if an upper bound was set
     * via {@link QueueBuilder#maxItems}.
     *
     * @param item item
     * @param itemId item Id
     * @param maxWait maximum wait
     * @param unit wait unit
     * @return true if items was added, false if timed out
     * @throws Exception
     */
    public boolean put(T item, String itemId, int maxWait, TimeUnit unit) throws Exception
    {
        Preconditions.checkArgument(isValidId(itemId), "Invalid id: " + itemId);

        queue.checkState();

        return queue.internalPut(item, null, makeIdPath(itemId), maxWait, unit);
    }

    /**
     * Remove any items with the given Id
     *
     * @param id item Id to remove
     * @return number of items removed
     * @throws Exception errors
     */
    public int remove(String id) throws Exception
    {
        id = Preconditions.checkNotNull(id, "id cannot be null");

        queue.checkState();

        int     count = 0;
        for ( String name : queue.getChildren() )
        {
            if ( parseId(name).id.equals(id) )
            {
                if ( queue.tryRemove(name) )
                {
                    ++count;
                }
            }
        }

        return count;
    }

    @VisibleForTesting
    boolean debugIsQueued(String id) throws Exception
    {
        for ( String name : queue.getChildren() )
        {
            if ( parseId(name).id.equals(id) )
            {
                return true;
            }
        }

        return false;
    }

    private String makeIdPath(String itemId)
    {
        return queue.makeItemPath() + SEPARATOR + fixId(itemId) + SEPARATOR;
    }

    private void internalSortChildren(List<String> children)
    {
        Collections.sort
        (
            children,
            new Comparator<String>()
            {
                @Override
                public int compare(String o1, String o2)
                {
                    return parseId(o1).cleaned.compareTo(parseId(o2).cleaned);
                }
            }
        );
    }
    
    private boolean isValidId(String id)
    {
        return (id != null) && (id.length() > 0);
    }

    private static String   fixId(String id)
    {
        String fixed = id.replace('/', '_');
        return fixed.replace(SEPARATOR, '_');
    }

    private Parts parseId(String name)
    {
        int         firstIndex = name.indexOf(SEPARATOR);
        int         secondIndex = name.indexOf(SEPARATOR, firstIndex + 1);
        if ( (firstIndex < 0) || (secondIndex < 0) )
        {
            log.error("Bad node in queue: " + name);
            return new Parts(name, name);
        }

        return new Parts
        (
            name.substring(firstIndex + 1, secondIndex),
            name.substring(0, firstIndex) + name.substring(secondIndex + 1)
        );
    }
}
