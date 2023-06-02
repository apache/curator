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

package org.apache.curator.framework.recipes.leader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ThreadUtils;
import org.junit.jupiter.api.Test;

public class TestLeaderSelectorWithExecutor extends BaseClassForTests {
    private static final ThreadFactory threadFactory = ThreadUtils.newThreadFactory("FeedGenerator");

    @Test
    public void test() throws Exception {
        Timing timing = new Timing();
        LeaderSelector leaderSelector = null;
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .retryPolicy(new ExponentialBackoffRetry(100, 3))
                .connectString(server.getConnectString())
                .sessionTimeoutMs(timing.session())
                .connectionTimeoutMs(timing.connection())
                .build();
        try {
            client.start();

            MyLeaderSelectorListener listener = new MyLeaderSelectorListener();
            ExecutorService executorPool = Executors.newFixedThreadPool(20);
            leaderSelector = new LeaderSelector(client, "/test", threadFactory, executorPool, listener);

            leaderSelector.autoRequeue();
            leaderSelector.start();

            timing.sleepABit();

            assertEquals(listener.getLeaderCount(), 1);
        } finally {
            CloseableUtils.closeQuietly(leaderSelector);
            CloseableUtils.closeQuietly(client);
        }
    }

    private class MyLeaderSelectorListener implements LeaderSelectorListener {
        private volatile Thread ourThread;
        private final AtomicInteger leaderCount = new AtomicInteger(0);

        public int getLeaderCount() {
            return leaderCount.get();
        }

        @Override
        public void takeLeadership(CuratorFramework curatorFramework) throws Exception {
            ourThread = Thread.currentThread();
            try {
                leaderCount.incrementAndGet();
                while (!Thread.currentThread().isInterrupted()) {
                    Thread.sleep(1000);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                leaderCount.decrementAndGet();
            }
        }

        @Override
        public void stateChanged(CuratorFramework curatorFramework, ConnectionState newState) {
            if ((newState == ConnectionState.LOST) || (newState == ConnectionState.SUSPENDED)) {
                if (ourThread != null) {
                    ourThread.interrupt();
                }
            }
        }
    }
}
