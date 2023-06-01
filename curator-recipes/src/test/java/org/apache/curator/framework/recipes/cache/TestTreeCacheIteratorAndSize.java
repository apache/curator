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

package org.apache.curator.framework.recipes.cache;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.google.common.collect.Sets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.compatibility.CuratorTestBase;
import org.junit.jupiter.api.Test;

public class TestTreeCacheIteratorAndSize extends CuratorTestBase {
    @Test
    public void testBasic() throws Exception {
        final String[] nodes = {
            "/base/test",
            "/base/test/3",
            "/base/test/3/0",
            "/base/test/3/0/0",
            "/base/test/3/0/1",
            "/base/test/3/1",
            "/base/test/3/1/0",
            "/base/test/3/1/1",
            "/base/test/3/2",
            "/base/test/3/2/0",
            "/base/test/3/2/1",
            "/base/test/3/2/3",
            "/base/test/3/3",
            "/base/test/3/3/1",
            "/base/test/3/3/3"
        };

        try (CuratorFramework client =
                CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1))) {
            client.start();

            String basePath = "/base/test";
            try (TreeCache treeCache = new TreeCache(client, basePath)) {
                treeCache.start();

                for (String node : nodes) {
                    client.create().creatingParentsIfNeeded().forPath(node, node.getBytes());
                }

                timing.sleepABit(); // let the cache settle

                Iterator<ChildData> iterator = treeCache.iterator();
                Map<String, byte[]> iteratorValues = new HashMap<>();
                while (iterator.hasNext()) {
                    ChildData next = iterator.next();
                    iteratorValues.put(next.getPath(), next.getData());
                }

                assertEquals(iteratorValues.size(), nodes.length);
                for (String node : nodes) {
                    assertArrayEquals(iteratorValues.get(node), node.getBytes());
                }

                assertEquals(treeCache.size(), nodes.length);
            }
        }
    }

    @Test
    public void testIteratorWithRandomGraph() throws Exception {
        Map<String, String> pathAndData = new HashMap<>();
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int nodeQty = random.nextInt(100, 200);
        int maxPerRow = random.nextInt(1, 10);
        int maxDepth = random.nextInt(3, 5);
        try (CuratorFramework client =
                CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1))) {
            client.start();

            String basePath = "/base/test";
            try (TreeCache treeCache = new TreeCache(client, basePath)) {
                treeCache.start();

                client.create().creatingParentsIfNeeded().forPath(basePath, "0".getBytes());
                pathAndData.put(basePath, "0");

                while (nodeQty-- > 0) {
                    int thisDepth = random.nextInt(1, maxDepth + 1);
                    StringBuilder path = new StringBuilder(basePath);
                    for (int i = 0; i < thisDepth; ++i) {
                        path.append("/").append(random.nextInt(maxPerRow));
                        long value = random.nextLong();
                        pathAndData.put(path.toString(), Long.toString(value));
                        client.create()
                                .orSetData()
                                .forPath(path.toString(), Long.toString(value).getBytes());
                    }
                }

                timing.sleepABit(); // let the cache settle

                assertEquals(treeCache.size(), pathAndData.size());

                // at this point we have a cached graph of random nodes with random values
                Iterator<ChildData> iterator = treeCache.iterator();
                while (iterator.hasNext()) {
                    ChildData next = iterator.next();
                    assertTrue(pathAndData.containsKey(next.getPath()));
                    assertArrayEquals(pathAndData.get(next.getPath()).getBytes(), next.getData());
                    pathAndData.remove(next.getPath());
                }

                assertEquals(pathAndData.size(), 0); // above loop should have removed all nodes
            }
        }
    }

    @Test
    public void testEmptyTree() throws Exception {
        try (CuratorFramework client =
                CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1))) {
            client.start();

            try (TreeCache treeCache = new TreeCache(client, "/base/test")) {
                treeCache.start();

                Iterator<ChildData> iterator = treeCache.iterator();
                assertFalse(iterator.hasNext());
                assertEquals(treeCache.size(), 0);
            }
        }
    }

    @Test
    public void testWithDeletedNodes() throws Exception {
        try (CuratorFramework client =
                CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1))) {
            client.start();

            try (TreeCache treeCache = new TreeCache(client, "/foo")) {
                treeCache.start();

                client.create().forPath("/foo");
                client.create().forPath("/foo/a1");
                client.create().forPath("/foo/a2");
                client.create().forPath("/foo/a2/a2.1");
                client.create().forPath("/foo/a2/a2.2");
                client.create().forPath("/foo/a3");
                client.create().forPath("/foo/a3/a3.1");
                client.create().forPath("/foo/a3/a3.2");

                client.delete().forPath("/foo/a2/a2.2");
                client.delete().forPath("/foo/a3/a3.1");

                timing.sleepABit(); // let the cache settle

                Iterator<ChildData> iterator = treeCache.iterator();
                Set<String> paths = new HashSet<>();
                while (iterator.hasNext()) {
                    ChildData next = iterator.next();
                    paths.add(next.getPath());
                }

                assertEquals(
                        paths,
                        Sets.newHashSet("/foo", "/foo/a1", "/foo/a2", "/foo/a2/a2.1", "/foo/a3", "/foo/a3/a3.2"));
                assertEquals(treeCache.size(), 6);
            }
        }
    }
}
