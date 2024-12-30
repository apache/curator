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

package org.apache.curator.framework.imps;

import java.util.concurrent.Callable;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.WatchersDebug;
import org.apache.curator.test.compatibility.Timing2;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.ZooKeeper;
import org.awaitility.Awaitility;

public class TestCleanState {
    private static final boolean IS_ENABLED = Boolean.getBoolean("PROPERTY_VALIDATE_NO_REMAINING_WATCHERS");

    public static void closeAndTestClean(CuratorFramework client) {
        if ((client == null) || !IS_ENABLED) {
            return;
        }

        try {
            Timing2 timing = new Timing2();
            InternalCuratorFramework internalClient = (InternalCuratorFramework) client;
            EnsembleTracker ensembleTracker = internalClient.getEnsembleTracker();
            if (ensembleTracker != null) {
                Awaitility.await().until(() -> !ensembleTracker.hasOutstanding());
                ensembleTracker.close();
            }
            ZooKeeper zooKeeper = internalClient.getZooKeeper();
            if (zooKeeper != null) {
                final int maxLoops = 3;
                for (int i = 0;
                        i < maxLoops;
                        ++i) // it takes time for the watcher removals to settle due to async/watchers, etc. So, if
                // there are remaining watchers, sleep a bit
                {
                    if (i > 0) {
                        timing.multiple(.5).sleepABit();
                    }
                    boolean isLast = (i + 1) == maxLoops;
                    if (WatchersDebug.getChildWatches(zooKeeper).size() != 0) {
                        if (isLast) {
                            throw new AssertionError("One or more child watchers are still registered: "
                                    + WatchersDebug.getChildWatches(zooKeeper));
                        }
                        continue;
                    }
                    if (WatchersDebug.getExistWatches(zooKeeper).size() != 0) {
                        if (isLast) {
                            throw new AssertionError("One or more exists watchers are still registered: "
                                    + WatchersDebug.getExistWatches(zooKeeper));
                        }
                        continue;
                    }
                    if (WatchersDebug.getDataWatches(zooKeeper).size() != 0) {
                        if (isLast) {
                            throw new AssertionError("One or more data watchers are still registered: "
                                    + WatchersDebug.getDataWatches(zooKeeper));
                        }
                        continue;
                    }
                    break;
                }
            }
        } catch (IllegalStateException ignore) {
            // client already closed
        } catch (Exception e) {
            e.printStackTrace(); // not sure what to do here
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    public static void test(CuratorFramework client, Callable<Void> proc) throws Exception {
        boolean succeeded = false;
        try {
            proc.call();
            succeeded = true;
        } finally {
            if (succeeded) {
                closeAndTestClean(client);
            } else {
                CloseableUtils.closeQuietly(client);
            }
        }
    }

    private TestCleanState() {}
}
