package com.netflix.curator.framework.recipes.queue;

import com.netflix.curator.framework.CuratorFramework;

public interface QueueAllocator<U, T extends QueueBase<U>>
{
    public T    allocateQueue(CuratorFramework client, String queuePath);
}
