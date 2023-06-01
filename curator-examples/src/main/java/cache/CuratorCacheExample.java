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

package cache;

import java.util.concurrent.ThreadLocalRandom;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;

/**
 * Very simple example of creating a CuratorCache that listens to events and logs the changes
 * to standard out. A loop of random changes is run to exercise the cache.
 */
public class CuratorCacheExample {
    private static final String PATH = "/example/cache";

    public static void main(String[] args) throws Exception {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        try (TestingServer server = new TestingServer()) {
            try (CuratorFramework client = CuratorFrameworkFactory.newClient(
                    server.getConnectString(), new ExponentialBackoffRetry(1000, 3))) {
                client.start();
                try (CuratorCache cache = CuratorCache.build(client, PATH)) {
                    // there are several ways to set a listener on a CuratorCache. You can watch for individual events
                    // or for all events. Here, we'll use the builder to log individual cache actions
                    CuratorCacheListener listener = CuratorCacheListener.builder()
                            .forCreates(node -> System.out.println(String.format("Node created: [%s]", node)))
                            .forChanges((oldNode, node) -> System.out.println(
                                    String.format("Node changed. Old: [%s] New: [%s]", oldNode, node)))
                            .forDeletes(oldNode ->
                                    System.out.println(String.format("Node deleted. Old value: [%s]", oldNode)))
                            .forInitialized(() -> System.out.println("Cache initialized"))
                            .build();

                    // register the listener
                    cache.listenable().addListener(listener);

                    // the cache must be started
                    cache.start();

                    // now randomly create/change/delete nodes
                    for (int i = 0; i < 1000; ++i) {
                        int depth = random.nextInt(1, 4);
                        String path = makeRandomPath(random, depth);
                        if (random.nextBoolean()) {
                            client.create()
                                    .orSetData()
                                    .creatingParentsIfNeeded()
                                    .forPath(
                                            path,
                                            Long.toString(random.nextLong()).getBytes());
                        } else {
                            client.delete().quietly().deletingChildrenIfNeeded().forPath(path);
                        }

                        Thread.sleep(5);
                    }
                }
            }
        }
    }

    private static String makeRandomPath(ThreadLocalRandom random, int depth) {
        if (depth == 0) {
            return PATH;
        }
        return makeRandomPath(random, depth - 1) + "/" + random.nextInt(3);
    }
}
