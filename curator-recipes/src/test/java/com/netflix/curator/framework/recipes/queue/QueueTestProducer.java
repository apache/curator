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

import java.util.concurrent.Callable;

public class QueueTestProducer implements Callable<Void>
{
    private final DistributedQueue<TestQueueItem> queue;
    private final int itemQty;
    private final int startIndex;

    public QueueTestProducer(DistributedQueue<TestQueueItem> queue, int itemQty, int startIndex)
    {
        this.queue = queue;
        this.itemQty = itemQty;
        this.startIndex = startIndex;
    }

    @Override
    public Void call() throws Exception
    {
        int     count = 0;
        while ( !Thread.currentThread().isInterrupted() && (count < itemQty) )
        {
            queue.put(new TestQueueItem(Integer.toString(count + startIndex)));
            ++count;
        }
        return null;
    }
}
