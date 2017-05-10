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
package org.apache.curator.x.async.modeled;

import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.test.Timing;
import org.apache.curator.x.async.modeled.cached.CachedModeledFramework;
import org.apache.curator.x.async.modeled.cached.ModeledCacheListener;
import org.apache.curator.x.async.modeled.models.TestModel;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.io.IOException;
import java.math.BigInteger;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

public class TestCachedModeledFramework extends TestModeledFrameworkBase
{
    @Test
    public void testThreading()
    {
        TestModel model = new TestModel("a", "b", "c", 1, BigInteger.ONE);
        CachedModeledFramework<TestModel> client = ModeledFramework.wrap(async, modelSpec).cached();

        CountDownLatch latch = new CountDownLatch(1);
        client.listenable().addListener((type, path1, stat, model1) -> latch.countDown());

        complete(client.set(model));
        client.start();
        try
        {
            Assert.assertTrue(new Timing().awaitLatch(latch));

            AtomicReference<Thread> completionThread = new AtomicReference<>();
            complete(client.read().whenComplete((s, e) -> completionThread.set((e == null) ? Thread.currentThread() : null)));
            Assert.assertNotNull(completionThread.get());
            Assert.assertNotEquals(Thread.currentThread(), completionThread.get(), "Should be different threads");
            completionThread.set(null);

            complete(client.at("foo").read().whenComplete((v, e) -> completionThread.set((e != null) ? Thread.currentThread() : null)));
            Assert.assertNotNull(completionThread.get());
            Assert.assertNotEquals(Thread.currentThread(), completionThread.get(), "Should be different threads");
            completionThread.set(null);
        }
        finally
        {
            client.close();
        }
    }

    @Test
    public void testCustomThreading()
    {
        AtomicReference<Thread> ourThread = new AtomicReference<>();
        ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
            Thread thread = new Thread(r, "testCustomThreading");
            ourThread.set(thread);
            return thread;
        });
        TestModel model = new TestModel("a", "b", "c", 1, BigInteger.ONE);
        CachedModeledFramework<TestModel> client = ModeledFramework.wrap(async, modelSpec).cached(executor);

        CountDownLatch latch = new CountDownLatch(1);
        client.listenable().addListener((type, path1, stat, model1) -> latch.countDown());

        complete(client.set(model));
        client.start();
        try
        {
            Assert.assertTrue(new Timing().awaitLatch(latch));

            AtomicReference<Thread> completionThread = new AtomicReference<>();
            complete(client.read().thenAccept(s -> completionThread.set(Thread.currentThread())));
            Assert.assertEquals(ourThread.get(), completionThread.get(), "Should be our thread");
            completionThread.set(null);

            complete(client.at("foo").read().whenComplete((v, e) -> completionThread.set((e != null) ? Thread.currentThread() : null)));
            Assert.assertEquals(ourThread.get(), completionThread.get(), "Should be our thread");
            completionThread.set(null);
        }
        finally
        {
            client.close();
        }
    }

    @Test
    public void testDownServer() throws IOException
    {
        Timing timing = new Timing();

        TestModel model = new TestModel("a", "b", "c", 1, BigInteger.ONE);
        CachedModeledFramework<TestModel> client = ModeledFramework.wrap(async, modelSpec).cached();
        Semaphore semaphore = new Semaphore(0);
        client.listenable().addListener((t, p, s, m) -> semaphore.release());

        client.start();
        try
        {
            client.set(model);
            Assert.assertTrue(timing.acquireSemaphore(semaphore));

            CountDownLatch latch = new CountDownLatch(1);
            rawClient.getConnectionStateListenable().addListener((__, state) -> {
                if ( state == ConnectionState.LOST )
                {
                    latch.countDown();
                }
            });
            server.stop();
            Assert.assertTrue(timing.awaitLatch(latch));

            complete(client.read().whenComplete((value, e) -> {
                Assert.assertNotNull(value);
                Assert.assertNull(e);
            }));
        }
        finally
        {
            client.close();
        }
    }

    @Test
    public void testPostInitializedFilter()
    {
        TestModel model1 = new TestModel("a", "b", "c", 1, BigInteger.ONE);
        TestModel model2 = new TestModel("d", "e", "e", 1, BigInteger.ONE);
        CachedModeledFramework<TestModel> client = ModeledFramework.wrap(async, modelSpec).cached();
        Semaphore semaphore = new Semaphore(0);
        ModeledCacheListener<TestModel> listener = (t, p, s, m) -> semaphore.release();
        client.listenable().addListener(listener.postInitializedOnly());

        complete(client.at("1").set(model1));  // set before cache is started
        client.start();
        try
        {
            Assert.assertFalse(timing.forSleepingABit().acquireSemaphore(semaphore));

            client.at("2").set(model2);  // set before cache is started
            Assert.assertTrue(timing.acquireSemaphore(semaphore));
        }
        finally
        {
            client.close();
        }
    }
}
