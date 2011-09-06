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
import com.netflix.curator.framework.CuratorFramework;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;

/**
 * The builder for both {@link DistributedQueue} and {@link DistributedPriorityQueue}
 * @param <T> item type for the queue
 */
public class QueueBuilder<T>
{
    private final CuratorFramework client;
    private final QueueSerializer<T> serializer;
    private final String queuePath;

    private ThreadFactory factory;
    private Executor executor;
    private int maxInternalQueue;
    private QueueSafety<T> queueSafety;

    /**
     * Allocate a new builder
     *
     * @param client the curator client
     * @param serializer serializer to use for items
     * @param queuePath path to store queue
     * @param <T> item type
     * @return builder
     */
    public static<T> QueueBuilder<T>        builder(CuratorFramework client, QueueSerializer<T> serializer, String queuePath)
    {
        return new QueueBuilder<T>(client, serializer, queuePath);
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
            serializer,
            queuePath,
            factory,
            executor,
            maxInternalQueue,
            Integer.MAX_VALUE,
            false,
            queueSafety
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
            serializer,
            queuePath,
            factory,
            executor,
            maxInternalQueue,
            minItemsBeforeRefresh,
            queueSafety
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
        Preconditions.checkNotNull(factory);

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
        Preconditions.checkNotNull(executor);

        this.executor = executor;
        return this;
    }

    /**
     * <p>Change the max internal queue size. The default is {@link Integer#MAX_VALUE}.</p>
     *
     * <p>Queue items are taken from ZooKeeper and stored in an internal Java list. <code>maxInternalQueue</code> is the
     * max length for that list. When full, new items will block until there's room in the list.</p>
     *
     * <p>NOTE: if you pass 0 for <code>maxInternalQueue</code>, a {@link SynchronousQueue} is used.</p>
     *
     * @param maxInternalQueue max internal list size
     * @return this
     */
    public QueueBuilder<T>  maxInternalQueue(int maxInternalQueue)
    {
        Preconditions.checkArgument(queueSafety == null, "Queue Safety is incompatible with maxInternalQueue");
        Preconditions.checkArgument(maxInternalQueue >= 0);

        this.maxInternalQueue = maxInternalQueue;
        return this;
    }

    /**
     * <p>Without a queue safety, messages are removed and put into an internal queue that is consumed by the client
     * application. If the application does not process the message for some reason (crash, etc.) the message is lost.</p>
     *
     * <p>Use a queue safety to make the message recoverable. The safety specifies a lock path used to hold a lock while
     * the message is being processed - this prevents other processes from taking the message. The safety also specifies
     * a consumer that the client app uses to process messages. IMPORTANT - do NOT use the {@link DistributedPriorityQueue#take()} or
     * {@link DistributedQueue#take()} methods. When using a safety, you MUST consume the message when {@link QueueSafetyConsumer#consumeMessage(Object)} is
     * called internally by the DistributedQueue.</p>
     *
     * @param queueSafety safety instances
     * @return this
     */
    public QueueBuilder<T>  queueSafety(QueueSafety<T> queueSafety)
    {
        Preconditions.checkNotNull(queueSafety);

        maxInternalQueue = 0;
        this.queueSafety = queueSafety;
        return this;
    }

    /**
     * Set the queue as being able to produce only. It will not check for messages to be consumed.
     *
     * @return this
     */
    public QueueBuilder<T>  makeProducerOnly()
    {
        maxInternalQueue = -1;
        return this;
    }

    private QueueBuilder(CuratorFramework client, QueueSerializer<T> serializer, String queuePath)
    {
        this.client = client;
        this.serializer = serializer;
        this.queuePath = queuePath;

        factory = Executors.defaultThreadFactory();
        executor = MoreExecutors.sameThreadExecutor();
        maxInternalQueue = Integer.MAX_VALUE;
    }
}
