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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.google.common.collect.Sets;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.test.Timing;
import org.apache.curator.test.compatibility.CuratorTestBase;
import org.apache.curator.x.async.modeled.cached.CachedModeledFramework;
import org.apache.curator.x.async.modeled.cached.ModeledCacheListener;
import org.apache.curator.x.async.modeled.models.TestModel;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Tag(CuratorTestBase.zk35TestCompatibilityGroup)
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
            assertTrue(timing.acquireSemaphore(semaphore));

            CountDownLatch latch = new CountDownLatch(1);
            rawClient.getConnectionStateListenable().addListener((__, state) -> {
                if ( state == ConnectionState.LOST )
                {
                    latch.countDown();
                }
            });
            server.stop();
            assertTrue(timing.awaitLatch(latch));

            complete(client.child(model).read().whenComplete((value, e) -> {
                assertNotNull(value);
                assertNull(e);
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
        TestModel model2 = new TestModel("d", "e", "f", 1, BigInteger.ONE);
        CachedModeledFramework<TestModel> client = ModeledFramework.wrap(async, modelSpec).cached();
        Semaphore semaphore = new Semaphore(0);
        ModeledCacheListener<TestModel> listener = (t, p, s, m) -> semaphore.release();
        client.listenable().addListener(listener.postInitializedOnly());

        complete(client.child("1").set(model1));  // set before cache is started
        client.start();
        try
        {
            assertFalse(timing.forSleepingABit().acquireSemaphore(semaphore));

            client.child("2").set(model2);  // set before cache is started
            assertTrue(timing.acquireSemaphore(semaphore));
        }
        finally
        {
            client.close();
        }
    }

    @Test
    public void testChildren()
    {
        TestModel parent = new TestModel("a", "b", "c", 20, BigInteger.ONE);
        TestModel child1 = new TestModel("d", "e", "f", 1, BigInteger.ONE);
        TestModel child2 = new TestModel("g", "h", "i", 1, BigInteger.ONE);
        TestModel grandChild1 = new TestModel("j", "k", "l", 10, BigInteger.ONE);
        TestModel grandChild2 = new TestModel("m", "n", "0", 5, BigInteger.ONE);

        try (CachedModeledFramework<TestModel> client = ModeledFramework.wrap(async, modelSpec).cached())
        {
            CountDownLatch latch = new CountDownLatch(5);
            client.listenable().addListener((t, p, s, m) -> latch.countDown());

            client.start();
            complete(client.child("p").set(parent));
            complete(client.child("p").child("c1").set(child1));
            complete(client.child("p").child("c2").set(child2));
            complete(client.child("p").child("c1").child("g1").set(grandChild1));
            complete(client.child("p").child("c2").child("g2").set(grandChild2));
            assertTrue(timing.awaitLatch(latch));

            complete(client.child("p").children(), (v, e) ->
            {
                List<ZPath> paths = Arrays.asList(
                    client.child("p").child("c1").modelSpec().path(),
                    client.child("p").child("c2").modelSpec().path()
                );
                assertEquals(v, paths);
            });

            complete(client.child("p").childrenAsZNodes(), (v, e) ->
            {
                Set<TestModel> cachedModels = toSet(v.stream(), ZNode::model);
                assertEquals(cachedModels, Sets.newHashSet(child1, child2));

                // verify that the same nodes are returned from the uncached method
                complete(ModeledFramework.wrap(async, modelSpec).child("p").childrenAsZNodes(), (v2, e2) -> {
                    Set<TestModel> uncachedModels = toSet(v2.stream(), ZNode::model);
                    assertEquals(cachedModels, uncachedModels);
                });
            });

            complete(client.child("p").child("c1").childrenAsZNodes(), (v, e) -> assertEquals(toSet(v.stream(), ZNode::model), Sets.newHashSet(grandChild1)));
            complete(client.child("p").child("c2").childrenAsZNodes(), (v, e) -> assertEquals(toSet(v.stream(), ZNode::model), Sets.newHashSet(grandChild2)));
        }
    }

    // note: CURATOR-546
    @Test
    public void testAccessCacheDirectly()
    {
        TestModel model = new TestModel("a", "b", "c", 20, BigInteger.ONE);
        try (CachedModeledFramework<TestModel> client = ModeledFramework.wrap(async, modelSpec).cached())
        {
            CountDownLatch latch = new CountDownLatch(1);
            client.listenable().addListener((t, p, s, m) -> latch.countDown());

            client.start();
            complete(client.child("m").set(model));
            assertTrue(timing.awaitLatch(latch));

            // call 2 times in a row to validate CURATOR-546
            Optional<ZNode<TestModel>> optZNode = client.cache().currentData(modelSpec.path().child("m"));
            assertEquals(optZNode.orElseThrow(() -> new AssertionError("node is missing")).model(), model);
            optZNode = client.cache().currentData(modelSpec.path().child("m"));
            assertEquals(optZNode.orElseThrow(() -> new AssertionError("node is missing")).model(), model);
        }
    }

    private <T, R> Set<R> toSet(Stream<T> stream, Function<? super T, ? extends R> mapper)
    {
         return stream.map(mapper).collect(Collectors.toSet());
    }
}
