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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import org.junit.jupiter.api.Test;

public class TestThreadUtils {
    @Test
    public void testFixedThreadPoolUsesBoundedQueueAndCallerRunsPolicy() {
        ExecutorService service = ThreadUtils.newFixedThreadPool(1, "test");
        try {
            ThreadPoolExecutor executor = (ThreadPoolExecutor) service;
            assertEquals(ThreadUtils.DEFAULT_THREAD_POOL_QUEUE_SIZE, executor.getQueue().remainingCapacity());
            assertTrue(executor.getRejectedExecutionHandler() instanceof ThreadPoolExecutor.CallerRunsPolicy);
        } finally {
            service.shutdownNow();
        }
    }
}
