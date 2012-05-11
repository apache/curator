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
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.curator.framework.CuratorFramework;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * The builder for both {@link DistributedQueue} and {@link DistributedPriorityQueue}
 * @param <T> item type for the queue
 */
public class QueueBuilder<T>
{
    private final CuratorFramework client;
    private final QueueConsumer<T> consumer;
    private final QueueSerializer<T> serializer;
    private final String queuePath;

    private ThreadFactory factory;
    private Executor executor;
    private String lockPath;

    private static final ThreadFactory defaultThreadFactory = new ThreadFactoryBuilder().setNameFormat("QueueBuilder-%d").build();

    /**
     * Allocate a new builder
     *
     *
     * @param client the curator client
     * @param consumer functor to consume messages - NOTE: pass <code>null</code> to make this a producer-only queue
     * @param serializer serializer to use for items
     * @param queuePath path to store queue
     * @return builder
     */
    public static<T> QueueBuilder<T>        builder(CuratorFramework client, QueueConsumer<T> consumer, QueueSerializer<T> serializer, String queuePath)
    {
        return new QueueBuilder<T>(client, consumer, serializer, queuePath);
    }

    /**
     * Build a {@link DistributedQueue} from the current builder values
     *
     * @return distributed queue
     */
    public DistributedQueue<T>      buildQueue()
    {
        return new DistributedQueue<T>
        (
            client,
            consumer,
            serializer,
            queuePath,
            factory,
            executor,
            Integer.MAX_VALUE,
            false,
            lockPath
        );
    }

    /**
     * Build a {@link DistributedIdQueue} from the current builder values
     *
     * @return distributed id queue
     */
    public DistributedIdQueue<T>      buildIdQueue()
    {
        return new DistributedIdQueue<T>
        (
            client,
            consumer,
            serializer,
            queuePath,
            factory,
            executor,
            Integer.MAX_VALUE,
            false,
            lockPath
        );
    }

    /**
     * <p>Build a {@link DistributedPriorityQueue} from the current builder values.</p>
     *
     * <p>When the priority
     * queue detects an item addition/removal, it will stop processing its current list of items and
     * refresh the list. <code>minItemsBeforeRefresh</code> modifies this. It determines the minimum
     * number of items from the active list that will get processed before a refresh.</p>
     *
     * <p>Due to a quirk in the way ZooKeeper notifies changes, the queue will get an item addition/remove
     * notification after <b>every</b> item is processed. This can lead to poor performance. Set
     * <code>minItemsBeforeRefresh</code> to the value your application can tolerate being out of sync.</p>
     *
     * <p>For example: if the queue sees 10 items to process, it will end up making 10 calls to ZooKeeper
     * to check status. You can control this by setting <code>minItemsBeforeRefresh</code> to 10 (or more)
     * and the queue will only refresh with ZooKeeper after 10 items are processed</p>
     *
     * @param minItemsBeforeRefresh minimum items to process before refreshing the item list
     * @return distributed priority queue
     */
    public DistributedPriorityQueue<T>      buildPriorityQueue(int minItemsBeforeRefresh)
    {
        return new DistributedPriorityQueue<T>
        (
            client,
            consumer,
            serializer,
            queuePath,
            factory,
            executor,
            minItemsBeforeRefresh,
            lockPath
        );
    }

    /**
     * Change the thread factory used. The default is {@link Executors#defaultThreadFactory()}
     *
     * @param factory new thread factory to use
     * @return this
     */
    public QueueBuilder<T>  threadFactory(ThreadFactory factory)
    {
        Preconditions.checkNotNull(factory, "factory cannot be null");

        this.factory = factory;
        return this;
    }

    /**
     * Change the executor used. The default is {@link MoreExecutors#sameThreadExecutor()}
     *
     * @param executor new executor to use
     * @return this
     */
    public QueueBuilder<T>  executor(Executor executor)
    {
        Preconditions.checkNotNull(executor, "executor cannot be null");

        this.executor = executor;
        return this;
    }

    /**
     * <p>Without a lock set, queue items are removed before being sent to the queue consumer. This can result in message
     * loss if the consumer fails to complete the message or the process dies.</p>
     *
     * <p>Use a lock to make the message recoverable. A lock is held while
     * the message is being processed - this prevents other processes from taking the message. The message will not be removed
     * from the queue until the consumer functor returns. Thus, if there is a failure or the process dies,
     * the message will get sent to another process. There is a small performance penalty for this behavior however.
     *
     * @param path path for the lock
     * @return this
     */
    public QueueBuilder<T>  lockPath(String path)
    {
        lockPath = path;
        return this;
    }

    private QueueBuilder(CuratorFramework client, QueueConsumer<T> consumer, QueueSerializer<T> serializer, String queuePath)
    {
        this.client = client;
        this.consumer = consumer;
        this.serializer = serializer;
        this.queuePath = queuePath;

        factory = defaultThreadFactory;
        executor = MoreExecutors.sameThreadExecutor();
    }
}
