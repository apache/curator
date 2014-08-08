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
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class BaseTestTreeCache extends BaseClassForTests
{
    private final Timing timing = new Timing();
    CuratorFramework client;
    MyTreeCache cache;
    private List<Throwable> exceptions;
    private BlockingQueue<TreeCacheEvent> events;
    TreeCacheListener eventListener;

    /**
     * A TreeCache that records exceptions and automatically adds a listener.
     */
    class MyTreeCache extends TreeCache
    {

        MyTreeCache(CuratorFramework client, String path, boolean cacheData)
        {
            super(client, path, cacheData);
            getListenable().addListener(eventListener);
        }

        @Override
        protected void handleException(Throwable e)
        {
            exceptions.add(e);
        }
    }

    @Override
    @BeforeMethod
    public void setup() throws Exception
    {
        super.setup();

        exceptions = new ArrayList<Throwable>();
        events = new LinkedBlockingQueue<TreeCacheEvent>();
        eventListener = new TreeCacheListener()
        {
            @Override
            public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception
            {
                if ( event.getData() != null && event.getData().getPath().startsWith("/zookeeper") )
                {
                    // Suppress any events related to /zookeeper paths
                    return;
                }
                events.add(event);
            }
        };

        initCuratorFramework();
    }

    void initCuratorFramework() {
        client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        client.start();
        client.getUnhandledErrorListenable().addListener(new UnhandledErrorListener()
        {
            @Override
            public void unhandledError(String message, Throwable e)
            {
                exceptions.add(e);
            }
        });
    }

    @Override
    @AfterMethod
    public void teardown() throws Exception
    {
        try
        {
            try
            {
                if ( exceptions.size() == 1 )
                {
                    Assert.fail("Exception was thrown", exceptions.get(0));
                }
                else if ( exceptions.size() > 1 )
                {
                    AssertionError error = new AssertionError("Multiple exceptions were thrown");
                    for ( Throwable exception : exceptions )
                    {
                        error.addSuppressed(exception);
                    }
                    throw error;
                }
            }
            finally
            {
                CloseableUtils.closeQuietly(cache);
                CloseableUtils.closeQuietly(client);
            }
        }
        finally
        {
            super.teardown();
        }
    }

    void assertNoMoreEvents() throws InterruptedException
    {
        timing.sleepABit();
        Assert.assertTrue(events.isEmpty());
    }

    TreeCacheEvent assertEvent(TreeCacheEvent.Type expectedType) throws InterruptedException
    {
        return assertEvent(expectedType, null);
    }

    TreeCacheEvent assertEvent(TreeCacheEvent.Type expectedType, String expectedPath) throws InterruptedException
    {
        return assertEvent(expectedType, expectedPath, null);
    }

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
