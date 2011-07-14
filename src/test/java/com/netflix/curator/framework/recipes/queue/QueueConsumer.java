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

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;

public class QueueConsumer implements Callable<Void>
{
    private final DistributedQueue<TestQueueItem>       queue;
    private final Queue<TestQueueItem>                  items = new ConcurrentLinkedQueue<TestQueueItem>();

    public QueueConsumer(DistributedQueue<TestQueueItem> queue)
    {
        this.queue = queue;
    }

    @Override
    public Void call() throws Exception
    {
        while ( !Thread.currentThread().isInterrupted() )
        {
            TestQueueItem item = queue.take();
            items.add(item);
        }
        return null;
    }

    public List<TestQueueItem> getItems()
    {
        return ImmutableList.copyOf(items);
    }

    public int  size()
    {
        return items.size();
    }
}
