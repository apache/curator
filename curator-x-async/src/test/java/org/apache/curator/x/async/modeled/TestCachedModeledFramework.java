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

import org.apache.curator.test.Timing;
import org.apache.curator.x.async.modeled.cached.CachedModeledFramework;
import org.apache.curator.x.async.modeled.models.TestModel;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.math.BigInteger;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

public class TestCachedModeledFramework extends TestModeledFramework
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
        Assert.assertTrue(new Timing().awaitLatch(latch));

        AtomicReference<Thread> completionThread = new AtomicReference<>();
        complete(client.read().whenComplete((s, e) -> completionThread.set((e == null) ? Thread.currentThread() : null)));
        Assert.assertEquals(ourThread.get(), completionThread.get(), "Should be our thread");
        completionThread.set(null);

        complete(client.at("foo").read().whenComplete((v, e) -> completionThread.set((e != null) ? Thread.currentThread() : null)));
        Assert.assertEquals(ourThread.get(), completionThread.get(), "Should be our thread");
        completionThread.set(null);
    }
}
