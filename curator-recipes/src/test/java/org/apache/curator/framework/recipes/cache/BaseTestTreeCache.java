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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.imps.TestCleanState;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public class BaseTestTreeCache extends BaseClassForTests {
    CuratorFramework client;
    TreeCache cache;
    protected final AtomicBoolean hadBackgroundException = new AtomicBoolean(false);
    private final BlockingQueue<TreeCacheEvent> events = new LinkedBlockingQueue<TreeCacheEvent>();
    private final Timing timing = new Timing();

    /**
     * Automatically records all events into an easily testable event stream.
     */
    final TreeCacheListener eventListener = new TreeCacheListener() {
        @Override
        public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
            // Suppress any events related to /zookeeper paths
            if (event.getData() != null && event.getData().getPath().startsWith("/zookeeper")) {
                return;
            }
            events.add(event);
        }
    };

    /**
     * Ensures that tests don't cause any background errors.
     */
    final UnhandledErrorListener errorListener = new UnhandledErrorListener() {
        @Override
        public void unhandledError(String message, Throwable e) {
            hadBackgroundException.set(true);
            e.printStackTrace(System.err);
        }
    };

    /**
     * Construct a TreeCache that records exceptions and automatically listens.
     */
    protected TreeCache newTreeCacheWithListeners(CuratorFramework client, String path) {
        TreeCache result = new TreeCache(client, path);
        result.getListenable().addListener(eventListener);
        result.getUnhandledErrorListenable().addListener(errorListener);
        return result;
    }

    /**
     * Finish constructing a TreeCache that records exceptions and automatically listens.
     */
    protected TreeCache buildWithListeners(TreeCache.Builder builder) {
        TreeCache result = builder.build();
        result.getListenable().addListener(eventListener);
        result.getUnhandledErrorListenable().addListener(errorListener);
        return result;
    }

    @Override
    @BeforeEach
    public void setup() throws Exception {
        super.setup();
        initCuratorFramework();
    }

    void initCuratorFramework() {
        client = CuratorFrameworkFactory.newClient(
                server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        client.start();
        client.getUnhandledErrorListenable().addListener(errorListener);
    }

    @Override
    @AfterEach
    public void teardown() throws Exception {
        try {
            try {
                assertFalse(hadBackgroundException.get(), "Background exceptions were thrown, see stderr for details");
                assertNoMoreEvents();
            } finally {
                CloseableUtils.closeQuietly(cache);
                TestCleanState.closeAndTestClean(client);
            }
        } finally {
            super.teardown();
        }
    }

    /**
     * Asserts the event queue is empty.
     */
    void assertNoMoreEvents() throws InterruptedException {
        timing.sleepABit();
        assertTrue(
                events.isEmpty(),
                String.format("Expected no events, found %d; first event: %s", events.size(), events.peek()));
    }

    /**
     * Asserts the given event is next in the queue, and consumes it from the queue.
     */
    TreeCacheEvent assertEvent(TreeCacheEvent.Type expectedType) throws InterruptedException {
        return assertEvent(expectedType, null);
    }

    /**
     * Asserts the given event is next in the queue, and consumes it from the queue.
     */
    TreeCacheEvent assertEvent(TreeCacheEvent.Type expectedType, String expectedPath) throws InterruptedException {
        return assertEvent(expectedType, expectedPath, null);
    }

    /**
     * Asserts the given event is next in the queue, and consumes it from the queue.
     */
    TreeCacheEvent assertEvent(TreeCacheEvent.Type expectedType, String expectedPath, byte[] expectedData)
            throws InterruptedException {
        return assertEvent(expectedType, expectedPath, expectedData, false);
    }

    TreeCacheEvent assertEvent(
            TreeCacheEvent.Type expectedType, String expectedPath, byte[] expectedData, boolean ignoreConnectionEvents)
            throws InterruptedException {
        TreeCacheEvent event = events.poll(timing.forWaiting().seconds(), TimeUnit.SECONDS);
        assertNotNull(event, String.format("Expected type: %s, path: %s", expectedType, expectedPath));
        if (ignoreConnectionEvents) {
            if ((event.getType() == TreeCacheEvent.Type.CONNECTION_SUSPENDED)
                    || (event.getType() == TreeCacheEvent.Type.CONNECTION_LOST)
                    || (event.getType() == TreeCacheEvent.Type.CONNECTION_RECONNECTED)) {
                return assertEvent(expectedType, expectedPath, expectedData, ignoreConnectionEvents);
            }
        }

        String message = event.toString();
        assertEquals(event.getType(), expectedType, message);
        if (expectedPath == null) {
            assertNull(event.getData(), message);
        } else {
            assertNotNull(event.getData(), message);
            assertEquals(event.getData().getPath(), expectedPath, message);
        }
        if (expectedData != null) {
            assertArrayEquals(event.getData().getData(), expectedData, message);
        }

        if (event.getType() == TreeCacheEvent.Type.NODE_UPDATED) {
            assertNotNull(event.getOldData());
        } else {
            assertNull(event.getOldData());
        }

        return event;
    }
}
