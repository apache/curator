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
package org.apache.curator.framework.recipes.watch;

import com.google.common.collect.Sets;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.TestCleanState;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.Timing;
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
    protected CuratorFramework client;
    protected CuratorCache cache;
    private final AtomicBoolean hadBackgroundException = new AtomicBoolean(false);
    private final BlockingQueue<EventAndNode> events = new LinkedBlockingQueue<>();
    private final Timing timing = new Timing();

    /**
     * Automatically records all events into an easily testable event stream.
     */
    final CacheListener eventListener = new CacheListener()
    {
        @Override
        public void process(CacheEvent event, String path, CachedNode affectedNode)
        {
            if ( path.startsWith("/zookeeper") )
            {
                return;
            }
            events.add(new EventAndNode(event, path, affectedNode));
        }
    };

    /**
     * Construct a TreeCache that records exceptions and automatically listens.
     */
    protected CuratorCache newTreeCacheWithListeners(CuratorFramework client, String path)
    {
        CuratorCache result = CuratorCacheBuilder.builder(client, path).build();
        result.getListenable().addListener(eventListener);
        return result;
    }

    /**
     * Finish constructing a TreeCache that records exceptions and automatically listens.
     */
    protected CuratorCache buildWithListeners(CuratorCacheBuilder builder)
    {
        CuratorCache result = builder.build();
        result.getListenable().addListener(eventListener);
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
                assertNoMoreEvents();
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
        Assert.assertTrue(events.isEmpty(), String.format("Expected no events, found %d; first event: %s", events.size(), events.peek()));
    }

    /**
     * Asserts the given event is next in the queue, and consumes it from the queue.
     */
    EventAndNode assertEvent(CacheEvent expectedType) throws InterruptedException
    {
        return assertEvent(expectedType, null);
    }

    /**
     * Asserts the given event is next in the queue, and consumes it from the queue.
     */
    EventAndNode assertEvent(CacheEvent expectedType, String expectedPath) throws InterruptedException
    {
        return assertEvent(expectedType, expectedPath, null);
    }

    /**
     * Asserts the given event is next in the queue, and consumes it from the queue.
     */
    EventAndNode assertEvent(CacheEvent expectedType, String expectedPath, byte[] expectedData) throws InterruptedException
    {
        EventAndNode event = events.poll(timing.forWaiting().seconds(), TimeUnit.SECONDS);
        Assert.assertNotNull(event, String.format("Expected type: %s, path: %s", expectedType, expectedPath));

        String message = event.toString();
        Assert.assertEquals(event.type, expectedType, message);
        if ( expectedPath == null )
        {
            Assert.assertEquals(expectedType, event.type);
        }
        else
        {
            Assert.assertNotNull(event.node, message);
            Assert.assertEquals(event.path, expectedPath, String.format("expected path: %s - actual path %s - message: %s", expectedPath, event.path, message));
        }
        if ( expectedData != null )
        {
            Assert.assertEquals(event.node.getData(), expectedData, message);
        }
        return event;
    }

    void assertChildNodeNames(String path, String... names)
    {
        if ( names == null )
        {
            names = new String[0];
        }
        Assert.assertEquals(Sets.newHashSet(cache.childNamesAtPath(path)), Sets.newHashSet(names));
    }
}
