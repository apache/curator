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
package org.apache.curator.x.async;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.Watcher;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;

import static java.util.EnumSet.of;
import static org.apache.curator.x.async.api.CreateOption.compress;
import static org.apache.curator.x.async.api.CreateOption.setDataIfExists;
import static org.apache.zookeeper.CreateMode.EPHEMERAL_SEQUENTIAL;

public class TestBasicOperations extends BaseClassForTests
{
    private static final Timing timing = new Timing();
    private AsyncCuratorFramework client;

    @BeforeMethod
    @Override
    public void setup() throws Exception
    {
        super.setup();

        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(timing.milliseconds()));
        client.start();
        this.client = AsyncCuratorFramework.wrap(client);
    }

    @AfterMethod
    @Override
    public void teardown() throws Exception
    {
        CloseableUtils.closeQuietly(client.unwrap());

        super.teardown();
    }

    @Test
    public void testCrud()
    {
        AsyncStage<String> createStage = client.create().forPath("/test", "one".getBytes());
        complete(createStage, (path, e) -> Assert.assertEquals(path, "/test"));

        AsyncStage<byte[]> getStage = client.getData().forPath("/test");
        complete(getStage, (data, e) -> Assert.assertEquals(data, "one".getBytes()));

        CompletionStage<byte[]> combinedStage = client.setData().forPath("/test", "new".getBytes()).thenCompose(
            __ -> client.getData().forPath("/test"));
        complete(combinedStage, (data, e) -> Assert.assertEquals(data, "new".getBytes()));

        CompletionStage<Void> combinedDelete = client.create().withMode(EPHEMERAL_SEQUENTIAL).forPath("/deleteme").thenCompose(
            path -> client.delete().forPath(path));
        complete(combinedDelete, (v, e) -> Assert.assertNull(e));

        CompletionStage<byte[]> setDataIfStage = client.create().withOptions(of(compress, setDataIfExists)).forPath("/test", "last".getBytes())
            .thenCompose(__ -> client.getData().decompressed().forPath("/test"));
        complete(setDataIfStage, (data, e) -> Assert.assertEquals(data, "last".getBytes()));
    }

    @Test
    public void testWatching()
    {
        CountDownLatch latch = new CountDownLatch(1);
        client.watched().checkExists().forPath("/test").event().whenComplete((event, exception) -> {
            Assert.assertNull(exception);
            Assert.assertEquals(event.getType(), Watcher.Event.EventType.NodeCreated);
            latch.countDown();
        });
        client.create().forPath("/test");
        Assert.assertTrue(timing.awaitLatch(latch));
    }

    private <T, U> void complete(CompletionStage<T> stage)
    {
        complete(stage, (v, e) -> {});
    }

    private <T, U> void complete(CompletionStage<T> stage, BiConsumer<? super T, Throwable> handler)
    {
        try
        {
            stage.handle((v, e) -> {
                handler.accept(v, e);
                return null;
            }).toCompletableFuture().get();
        }
        catch ( InterruptedException e )
        {
            Thread.interrupted();
        }
        catch ( ExecutionException e )
        {
            if ( e.getCause() instanceof AssertionError )
            {
                throw (AssertionError)e.getCause();
            }
            Assert.fail("get() failed", e);
        }
    }
}
