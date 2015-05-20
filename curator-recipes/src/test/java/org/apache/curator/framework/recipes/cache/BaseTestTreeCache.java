/**
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

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.imps.TestCleanState;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableExecutorService;
import org.apache.curator.utils.CloseableUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class BaseTestTreeCache extends BaseClassForTests
{
    CuratorFramework client;
    TreeCache cache;
    private final AtomicBoolean hadBackgroundException = new AtomicBoolean(false);
    private final BlockingQueue<TreeCacheEvent> events = new LinkedBlockingQueue<TreeCacheEvent>();
    private final Timing timing = new Timing();

    /**
     * Automatically records all events into an easily testable event stream.
     */
    final TreeCacheListener eventListener = new TreeCacheListener()
    {
        @Override
        public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception
        {
            // Suppress any events related to /zookeeper paths
            if ( event.getData() != null && event.getData().getPath().startsWith("/zookeeper") )
            {
                return;
            }
            events.add(event);
        }
    };

    /**
     * Ensures that tests don't cause any background errors.
     */
    final UnhandledErrorListener errorListener = new UnhandledErrorListener()
    {
        @Override
        public void unhandledError(String message, Throwable e)
        {
            hadBackgroundException.set(true);
            e.printStackTrace(System.err);
        }
    };

    /**
     * Construct a TreeCache that records exceptions and automatically listens.
     */
    protected TreeCache newTreeCacheWithListeners(CuratorFramework client, String path)
    {
        TreeCache result = new TreeCache(client, path);
        result.getListenable().addListener(eventListener);
        result.getUnhandledErrorListenable().addListener(errorListener);
        return result;
    }

    /**
     * Finish constructing a TreeCache that records exceptions and automatically listens.
     */
    protected TreeCache buildWithListeners(TreeCache.Builder builder)
    {
        TreeCache result = builder.build();
        result.getListenable().addListener(eventListener);
        result.getUnhandledErrorListenable().addListener(errorListener);
        return result;
    }

    @Override
    @BeforeMethod
    public void setup() throws Exception
    {
        super.setup();
        initCuratorFramework();
    }

    void initCuratorFramework()
    {
        client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        client.start();
        client.getUnhandledErrorListenable().addListener(errorListener);
    }

    @Override
    @AfterMethod
    public void teardown() throws Exception
    {
        try
        {
            try
            {
                Assert.assertFalse(hadBackgroundException.get(), "Background exceptions were thrown, see stderr for details");
            }
            finally
            {
                CloseableUtils.closeQuietly(cache);
                TestCleanState.closeAndTestClean(client);
            }
        }
        finally
        {
            super.teardown();
        }
    }

    /**
     * Asserts the event queue is empty.
     */
    void assertNoMoreEvents() throws InterruptedException
    {
        timing.sleepABit();
        Assert.assertTrue(events.isEmpty());
    }

    /**
     * Asserts the given event is next in the queue, and consumes it from the queue.
     */
    TreeCacheEvent assertEvent(TreeCacheEvent.Type expectedType) throws InterruptedException
    {
        return assertEvent(expectedType, null);
    }

    /**
     * Asserts the given event is next in the queue, and consumes it from the queue.
     */
    TreeCacheEvent assertEvent(TreeCacheEvent.Type expectedType, String expectedPath) throws InterruptedException
    {
        return assertEvent(expectedType, expectedPath, null);
    }

    /**
     * Asserts the given event is next in the queue, and consumes it from the queue.
     */
    TreeCacheEvent assertEvent(TreeCacheEvent.Type expectedType, String expectedPath, byte[] expectedData) throws InterruptedException
    {
        TreeCacheEvent event = events.poll(timing.forWaiting().seconds(), TimeUnit.SECONDS);
        Assert.assertNotNull(event, String.format("Expected type: %s, path: %s", expectedType, expectedPath));

        String message = event.toString();
        Assert.assertEquals(event.getType(), expectedType, message);
        if ( expectedPath == null )
        {
            Assert.assertNull(event.getData(), message);
        }
        else
        {
            Assert.assertNotNull(event.getData(), message);
            Assert.assertEquals(event.getData().getPath(), expectedPath, message);
        }
        if ( expectedData != null )
        {
            Assert.assertEquals(event.getData().getData(), expectedData, message);
        }
        return event;
    }
}
