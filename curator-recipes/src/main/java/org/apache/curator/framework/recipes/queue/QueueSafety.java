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

import java.util.concurrent.BlockingQueue;

/**
 * Parameter block for specifying queue safety with either {@link DistributedQueue} or
 * {@link DistributedPriorityQueue}
 */
public class QueueSafety<T>
{
    private final String lockPath;
    private final QueueConsumer<T> consumer;
    private final BlockingQueue<T> queue;

    /**
     * @param lockPath ZKPath to use for locking purposes
     * @param consumer the message consumer
     */
    public QueueSafety(String lockPath, QueueConsumer<T> consumer)
    {
        this.lockPath = lockPath;
        this.consumer = consumer;
        this.queue = null;
    }

    QueueSafety(String lockPath, BlockingQueue<T> queue)
    {
        this.lockPath = lockPath;
        this.consumer = null;
        this.queue = queue;
    }

    String getLockPath()
    {
        return lockPath;
    }

    QueueConsumer<T> getConsumer()
    {
        return consumer;
    }

    BlockingQueue<T> getQueue()
    {
        return queue;
    }
}
