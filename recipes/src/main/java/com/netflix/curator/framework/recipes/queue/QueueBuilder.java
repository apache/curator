package com.netflix.curator.framework.recipes.queue;

import com.google.common.util.concurrent.MoreExecutors;
import com.netflix.curator.framework.CuratorFramework;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class QueueBuilder<T>
{
    private final CuratorFramework client;
    private final QueueSerializer<T> serializer;
    private final String queuePath;

    private ThreadFactory factory;
    private Executor executor;
    private int maxInternalQueue;

    public static<T> QueueBuilder<T>        builder(CuratorFramework client, QueueSerializer<T> serializer, String queuePath)
    {
        return new QueueBuilder<T>(client, serializer, queuePath);
    }

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
            false
        );
    }

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
            minItemsBeforeRefresh
        );
    }

    public QueueBuilder(CuratorFramework client, QueueSerializer<T> serializer, String queuePath)
    {
        this.client = client;
        this.serializer = serializer;
        this.queuePath = queuePath;

        factory = Executors.defaultThreadFactory();
        executor = MoreExecutors.sameThreadExecutor();
        maxInternalQueue = Integer.MAX_VALUE;
    }

    public QueueBuilder<T>  threadFactory(ThreadFactory factory)
    {
        this.factory = factory;
        return this;
    }

    public QueueBuilder<T>  executor(Executor executor)
    {
        this.executor = executor;
        return this;
    }

    public QueueBuilder<T>  maxInternalQueue(int maxInternalQueue)
    {
        this.maxInternalQueue = maxInternalQueue;
        return this;
    }
}
