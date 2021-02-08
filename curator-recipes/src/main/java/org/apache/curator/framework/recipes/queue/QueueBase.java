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

import org.apache.curator.framework.listen.Listenable;
import java.io.Closeable;
import java.util.concurrent.TimeUnit;

public interface QueueBase<T> extends Closeable
{
    /**
     * Start the queue. No other methods work until this is called
     *
     * @throws Exception startup errors
     */
    void     start() throws Exception;

    /**
     * Return the manager for put listeners
     *
     * @return put listener container
     */
    Listenable<QueuePutListener<T>> getPutListenerContainer();

    /**
     * Used when the queue is created with a {@link QueueBuilder#lockPath(String)}. Determines
     * the behavior when the queue consumer throws an exception
     *
     * @param newErrorMode the new error mode (the default is {@link ErrorMode#REQUEUE}
     */
    void     setErrorMode(ErrorMode newErrorMode);

    /**
     * Wait until any pending puts are committed
     *
     * @param waitTime max wait time
     * @param timeUnit time unit
     * @return true if the flush was successful, false if it timed out first
     * @throws InterruptedException if thread was interrupted
     */
    boolean flushPuts(long waitTime, TimeUnit timeUnit) throws InterruptedException;

    /**
     * Return the most recent message count from the queue. This is useful for debugging/information
     * purposes only.
     *
     * @return count (can be 0)
     */
    int getLastMessageCount();
}
