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
import java.util.concurrent.Semaphore;

public class TestCachedModeledFramework extends TestModeledFrameworkBase
{
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
            client.child(model).set(model);
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

            complete(client.child(model).read().whenComplete((value, e) -> {
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

        complete(client.child("1").set(model1));  // set before cache is started
        client.start();
        try
        {
            Assert.assertFalse(timing.forSleepingABit().acquireSemaphore(semaphore));

            client.child("2").set(model2);  // set before cache is started
            Assert.assertTrue(timing.acquireSemaphore(semaphore));
        }
        finally
        {
            client.close();
        }
    }
}
