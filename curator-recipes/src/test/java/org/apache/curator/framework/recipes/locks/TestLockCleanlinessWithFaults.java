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

package org.apache.curator.framework.recipes.locks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import java.util.List;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.TestCleanState;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.BaseClassForTests;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.Test;

public class TestLockCleanlinessWithFaults extends BaseClassForTests {
    @Test
    public void testNodeDeleted() throws Exception {
        final String PATH = "/foo/bar";

        CuratorFramework client = null;
        try {
            client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryNTimes(0, 0));
            client.start();

            client.create().creatingParentsIfNeeded().forPath(PATH);
            assertEquals(client.checkExists().forPath(PATH).getNumChildren(), 0);

            LockInternals internals = new LockInternals(client, new StandardLockInternalsDriver(), PATH, "lock-", 1) {
                @Override
                List<String> getSortedChildren() throws Exception {
                    throw new KeeperException.NoNodeException();
                }
            };
            try {
                internals.attemptLock(0, null, null);
                fail();
            } catch (KeeperException.NoNodeException dummy) {
                // expected
            }

            // make sure no nodes are left lying around
            assertEquals(client.checkExists().forPath(PATH).getNumChildren(), 0);
        } finally {
            TestCleanState.closeAndTestClean(client);
        }
    }
}
