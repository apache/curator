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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.curator.ensemble.fixed.FixedEnsembleProvider;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingCluster;
import org.apache.curator.test.compatibility.Timing2;
import org.junit.jupiter.api.Test;

public class TestLongNetworkPartition {
    private static final Timing2 timing = new Timing2();

    // test for https://issues.apache.org/jira/browse/CURATOR-623
    @Test
    public void testLongNetworkPartition() throws Exception {
        final CompletableFuture<Void> done = new CompletableFuture<>();
        try (final TestingCluster testingCluster = started(new TestingCluster(1));
                final CuratorFramework dyingCuratorFramework = getCuratorFramework(testingCluster.getConnectString());
                final DistributedQueue<String> dyingQueue = newQueue(dyingCuratorFramework, item -> {
                    if (item.equals("0")) {
                        done.complete(null);
                    }
                })) {
            dyingQueue.start();
            testingCluster.killServer(testingCluster.getInstances().iterator().next());
            timing.forSessionSleep().multiple(2).sleep();
            testingCluster.restartServer(
                    testingCluster.getInstances().iterator().next());
            try (final CuratorFramework aliveCuratorFramework = getCuratorFramework(testingCluster.getConnectString());
                    final DistributedQueue<String> aliveQueue = newQueue(aliveCuratorFramework, null)) {
                aliveQueue.start();
                aliveQueue.put("0");
                done.get(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS);
            }
        }
    }

    private static DistributedQueue<String> newQueue(CuratorFramework curatorFramework, Consumer<String> consumer) {
        curatorFramework.start();
        return QueueBuilder.builder(
                        curatorFramework,
                        consumer == null
                                ? null
                                : new QueueConsumer<String>() {
                                    @Override
                                    public void consumeMessage(String o) {
                                        consumer.accept(o);
                                    }

                                    @Override
                                    public void stateChanged(
                                            CuratorFramework curatorFramework, ConnectionState connectionState) {}
                                },
                        new QueueSerializer<String>() {
                            @Override
                            public byte[] serialize(String item) {
                                return item.getBytes();
                            }

                            @Override
                            public String deserialize(byte[] bytes) {
                                return new String(bytes);
                            }
                        },
                        "/MyChildrenCacheTest/queue")
                .buildQueue();
    }

    private static TestingCluster started(TestingCluster testingCluster) throws Exception {
        testingCluster.start();
        return testingCluster;
    }

    private static CuratorFramework getCuratorFramework(String connectString) {
        return CuratorFrameworkFactory.builder()
                .ensembleProvider(new FixedEnsembleProvider(connectString, true))
                .sessionTimeoutMs(timing.session())
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();
    }
}
