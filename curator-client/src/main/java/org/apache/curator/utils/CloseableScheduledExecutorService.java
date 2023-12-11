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

package org.apache.curator.utils;

import com.google.common.base.Preconditions;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Decoration on an ScheduledExecutorService that tracks created futures and provides
 * a method to close futures created via this class
 */
public class CloseableScheduledExecutorService extends CloseableExecutorService {
    private final ScheduledExecutorService scheduledExecutorService;

    /**
     * @param scheduledExecutorService the service to decorate
     */
    public CloseableScheduledExecutorService(ScheduledExecutorService scheduledExecutorService) {
        super(scheduledExecutorService, false);
        this.scheduledExecutorService = scheduledExecutorService;
    }

    /**
     * @param scheduledExecutorService the service to decorate
     * @param shutdownOnClose if true, shutdown the executor service when this is closed
     */
    public CloseableScheduledExecutorService(
            ScheduledExecutorService scheduledExecutorService, boolean shutdownOnClose) {
        super(scheduledExecutorService, shutdownOnClose);
        this.scheduledExecutorService = scheduledExecutorService;
    }

    /**
     * Creates and executes a one-shot action that becomes enabled
     * after the given delay.
     *
     * @param task  the task to execute
     * @param delay the time from now to delay execution
     * @param unit  the time unit of the delay parameter
     * @return a Future representing pending completion of
     *         the task and whose <tt>get()</tt> method will return
     *         <tt>null</tt> upon completion
     */
    public Future<?> schedule(Runnable task, long delay, TimeUnit unit) {
        Preconditions.checkState(isOpen.get(), "CloseableExecutorService is closed");

        InternalFutureTask<Void> futureTask = new InternalFutureTask<Void>(new FutureTask<Void>(task, null));
        scheduledExecutorService.schedule(futureTask, delay, unit);
        return futureTask;
    }

    /**
     * Creates and executes a periodic action that becomes enabled first
     * after the given initial delay, and subsequently with the
     * given delay between the termination of one execution and the
     * commencement of the next.  If any execution of the task
     * encounters an exception, subsequent executions are suppressed.
     * Otherwise, the task will only terminate via cancellation or
     * termination of the executor.
     *
     * @param task      the task to execute
     * @param initialDelay the time to delay first execution
     * @param delay        the delay between the termination of one
     *                     execution and the commencement of the next
     * @param unit         the time unit of the initialDelay and delay parameters
     * @return a Future representing pending completion of
     *         the task, and whose <tt>get()</tt> method will throw an
     *         exception upon cancellation
     */
    public Future<?> scheduleWithFixedDelay(Runnable task, long initialDelay, long delay, TimeUnit unit) {
        Preconditions.checkState(isOpen.get(), "CloseableExecutorService is closed");

        ScheduledFuture<?> scheduledFuture =
                scheduledExecutorService.scheduleWithFixedDelay(task, initialDelay, delay, unit);
        return new InternalScheduledFutureTask(scheduledFuture);
    }
}
