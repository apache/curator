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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestCloseableScheduledExecutorService {
    private static final int QTY = 10;
    private static final int DELAY_MS = 100;

    private volatile ScheduledExecutorService executorService;

    @BeforeEach
    public void setup() {
        executorService = Executors.newScheduledThreadPool(QTY * 2);
    }

    @AfterEach
    public void tearDown() {
        executorService.shutdownNow();
    }

    @Test
    public void testCloseableScheduleWithFixedDelay() throws InterruptedException {
        CloseableScheduledExecutorService service = new CloseableScheduledExecutorService(executorService);

        final CountDownLatch latch = new CountDownLatch(QTY);
        service.scheduleWithFixedDelay(
                new Runnable() {
                    @Override
                    public void run() {
                        latch.countDown();
                    }
                },
                DELAY_MS,
                DELAY_MS,
                TimeUnit.MILLISECONDS);

        assertTrue(latch.await((QTY * 2) * DELAY_MS, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testCloseableScheduleWithFixedDelayAndAdditionalTasks() throws InterruptedException {
        final AtomicInteger outerCounter = new AtomicInteger(0);
        Runnable command = new Runnable() {
            @Override
            public void run() {
                outerCounter.incrementAndGet();
            }
        };
        executorService.scheduleWithFixedDelay(command, DELAY_MS, DELAY_MS, TimeUnit.MILLISECONDS);

        CloseableScheduledExecutorService service = new CloseableScheduledExecutorService(executorService);

        final AtomicInteger innerCounter = new AtomicInteger(0);
        service.scheduleWithFixedDelay(
                new Runnable() {
                    @Override
                    public void run() {
                        innerCounter.incrementAndGet();
                    }
                },
                DELAY_MS,
                DELAY_MS,
                TimeUnit.MILLISECONDS);

        Thread.sleep(DELAY_MS * 4);

        service.close();
        Thread.sleep(DELAY_MS * 2);

        int innerValue = innerCounter.get();
        assertTrue(innerValue > 0);

        int value = outerCounter.get();
        Thread.sleep(DELAY_MS * 2);
        int newValue = outerCounter.get();
        assertTrue(newValue > value);
        assertEquals(innerValue, innerCounter.get());

        value = newValue;
        Thread.sleep(DELAY_MS * 2);
        newValue = outerCounter.get();
        assertTrue(newValue > value);
        assertEquals(innerValue, innerCounter.get());
    }
}
