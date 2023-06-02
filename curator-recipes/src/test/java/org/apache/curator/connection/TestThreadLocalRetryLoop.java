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

package org.apache.curator.connection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.curator.RetryLoop;
import org.apache.curator.RetryPolicy;
import org.apache.curator.RetrySleeper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.compatibility.CuratorTestBase;
import org.apache.curator.utils.ThreadUtils;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class TestThreadLocalRetryLoop extends CuratorTestBase {
    private static final int retryCount = 4;
    private static final String backgroundThreadNameBase = "ignore-curator-background-thread";

    @Test
    @DisplayName("Check for fix for CURATOR-559")
    public void testRecursingRetry() throws Exception {
        AtomicInteger count = new AtomicInteger();
        try (CuratorFramework client = newClient(count)) {
            prep(client, count);
            doOperation(client);
            assertEquals(
                    count.get(),
                    retryCount
                            + 1); // Curator's retry policy has been off by 1 since inception - we might consider fixing
            // it someday
        }
    }

    @Test
    @DisplayName("Check for fix for CURATOR-559 with multiple threads")
    public void testThreadedRecursingRetry() throws Exception {
        final int threadQty = 4;
        ExecutorService executorService = Executors.newFixedThreadPool(threadQty);
        AtomicInteger count = new AtomicInteger();
        try (CuratorFramework client = newClient(count)) {
            prep(client, count);
            for (int i = 0; i < threadQty; ++i) {
                executorService.submit(() -> doOperation(client));
            }
            executorService.shutdown();
            assertTrue(executorService.awaitTermination(timing.milliseconds(), TimeUnit.MILLISECONDS));
            assertEquals(
                    count.get(),
                    threadQty
                            * (retryCount + 1)); // Curator's retry policy has been off by 1 since inception - we might
            // consider fixing it someday
        }
    }

    @Test
    public void testBadReleaseWithNoGet() {
        assertThrows(NullPointerException.class, () -> {
            ThreadLocalRetryLoop retryLoopStack = new ThreadLocalRetryLoop();
            retryLoopStack.release();
        });
    }

    private CuratorFramework newClient(AtomicInteger count) {
        RetryPolicy retryPolicy = makeRetryPolicy(count);
        return CuratorFrameworkFactory.builder()
                .connectString(server.getConnectString())
                .connectionTimeoutMs(100)
                .sessionTimeoutMs(100)
                .retryPolicy(retryPolicy)
                .threadFactory(ThreadUtils.newThreadFactory(backgroundThreadNameBase))
                .build();
    }

    private void prep(CuratorFramework client, AtomicInteger count) throws Exception {
        client.start();
        client.create().forPath("/test");
        CountDownLatch lostLatch = new CountDownLatch(1);
        client.getConnectionStateListenable().addListener((__, newState) -> {
            if (newState == ConnectionState.LOST) {
                lostLatch.countDown();
            }
        });
        server.stop();
        assertTrue(timing.awaitLatch(lostLatch));
        count.set(0); // in case the server shutdown incremented the count
    }

    private Void doOperation(CuratorFramework client) throws Exception {
        try {
            RetryLoop.callWithRetry(client.getZookeeperClient(), () -> {
                client.checkExists().forPath("/hey");
                return null;
            });
            fail("Should have thrown an exception");
        } catch (KeeperException dummy) {
            // correct
        }
        return null;
    }

    private RetryPolicy makeRetryPolicy(AtomicInteger count) {
        return new RetryNTimes(retryCount, 1) {
            @Override
            public boolean allowRetry(int retryCount, long elapsedTimeMs, RetrySleeper sleeper) {
                if (!Thread.currentThread()
                        .getName()
                        .contains(
                                backgroundThreadNameBase)) // if it does, it's Curator's background thread - don't count
                // these
                {
                    count.incrementAndGet();
                }
                return super.allowRetry(retryCount, elapsedTimeMs, sleeper);
            }
        };
    }
}
