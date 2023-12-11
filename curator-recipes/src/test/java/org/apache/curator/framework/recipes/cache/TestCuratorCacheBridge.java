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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.compatibility.CuratorTestBase;
import org.apache.curator.utils.Compatibility;
import org.junit.jupiter.api.Test;

public class TestCuratorCacheBridge extends CuratorTestBase {
    @Test
    public void testImplementationSelection() {
        try (CuratorFramework client =
                CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1))) {
            CuratorCacheBridge cache =
                    CuratorCache.bridgeBuilder(client, "/foo").build();
            if (Compatibility.hasPersistentWatchers()) {
                assertTrue(cache instanceof CuratorCacheImpl);
                assertTrue(cache.isCuratorCache());
            } else {
                assertTrue(cache instanceof CompatibleCuratorCacheBridge);
                assertFalse(cache.isCuratorCache());
            }
        }
    }

    @Test
    public void testForceTreeCache() {
        System.setProperty("curator-cache-bridge-force-tree-cache", "true");
        try (CuratorFramework client =
                CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1))) {
            CuratorCacheBridge cache =
                    CuratorCache.bridgeBuilder(client, "/foo").build();
            assertTrue(cache instanceof CompatibleCuratorCacheBridge);
            assertFalse(cache.isCuratorCache());
        } finally {
            System.clearProperty("curator-cache-bridge-force-tree-cache");
        }
    }
}
