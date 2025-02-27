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

package org.apache.curator.x.async.modeled;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.test.Timing;
import org.apache.curator.test.compatibility.CuratorTestBase;
import org.apache.curator.x.async.AsyncStage;
import org.apache.curator.x.async.modeled.cached.CachedModeledFramework;
import org.apache.curator.x.async.modeled.cached.ModeledCacheListener;
import org.apache.curator.x.async.modeled.models.TestModel;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@Tag(CuratorTestBase.zk35TestCompatibilityGroup)
public class TestCachedModeledFramework extends TestModeledFrameworkBase {
    enum CachedModeledFrameworkType {
        UNINITIALIZED,
        INITIALIZED
    }

    @ParameterizedTest
    @EnumSource(CachedModeledFrameworkType.class)
    void testDownServer(CachedModeledFrameworkType type) throws IOException {
        Timing timing = new Timing();

        TestModel model = new TestModel("a", "b", "c", 1, BigInteger.ONE);
        CachedModeledFramework<TestModel> client = build(type, modelSpec);
        Semaphore semaphore = new Semaphore(0);
        client.listenable().addListener((t, p, s, m) -> semaphore.release());

        client.start();
        try {
            client.child(model).set(model);
            assertTrue(timing.acquireSemaphore(semaphore));

            CountDownLatch latch = new CountDownLatch(1);
            rawClient.getConnectionStateListenable().addListener((__, state) -> {
                if (state == ConnectionState.LOST) {
                    latch.countDown();
                }
            });
            server.stop();
            assertTrue(timing.awaitLatch(latch));

            complete(client.child(model).read().whenComplete((value, e) -> {
                assertNotNull(value);
                assertNull(e);
            }));
        } finally {
            client.close();
        }
    }

    @ParameterizedTest
    @EnumSource(CachedModeledFrameworkType.class)
    void testPostInitializedFilter(CachedModeledFrameworkType type) {
        TestModel model1 = new TestModel("a", "b", "c", 1, BigInteger.ONE);
        TestModel model2 = new TestModel("d", "e", "f", 1, BigInteger.ONE);
        CachedModeledFramework<TestModel> client = build(type, modelSpec);
        Semaphore semaphore = new Semaphore(0);
        ModeledCacheListener<TestModel> listener = (t, p, s, m) -> semaphore.release();
        client.listenable().addListener(listener.postInitializedOnly());

        complete(client.child("1").set(model1)); // set before cache is started
        client.start();
        try {
            assertFalse(timing.forSleepingABit().acquireSemaphore(semaphore));

            client.child("2").set(model2); // set before cache is started
            assertTrue(timing.acquireSemaphore(semaphore));
        } finally {
            client.close();
        }
    }

    @ParameterizedTest
    @EnumSource(CachedModeledFrameworkType.class)
    void testChildren(CachedModeledFrameworkType type) {
        TestModel parent = new TestModel("a", "b", "c", 20, BigInteger.ONE);
        TestModel child1 = new TestModel("d", "e", "f", 1, BigInteger.ONE);
        TestModel child2 = new TestModel("g", "h", "i", 1, BigInteger.ONE);
        TestModel grandChild1 = new TestModel("j", "k", "l", 10, BigInteger.ONE);
        TestModel grandChild2 = new TestModel("m", "n", "0", 5, BigInteger.ONE);

        try (CachedModeledFramework<TestModel> client = build(type, modelSpec)) {
            CountDownLatch latch = new CountDownLatch(5);
            client.listenable().addListener((t, p, s, m) -> latch.countDown());

            client.start();
            complete(client.child("p").set(parent));
            complete(client.child("p").child("c1").set(child1));
            complete(client.child("p").child("c2").set(child2));
            complete(client.child("p").child("c1").child("g1").set(grandChild1));
            complete(client.child("p").child("c2").child("g2").set(grandChild2));
            assertTrue(timing.awaitLatch(latch));

            complete(client.child("p").children(), (v, e) -> {
                List<ZPath> paths = Arrays.asList(
                        client.child("p").child("c1").modelSpec().path(),
                        client.child("p").child("c2").modelSpec().path());
                assertEquals(v, paths);
            });

            complete(client.child("p").childrenAsZNodes(), (v, e) -> {
                Set<TestModel> cachedModels = toSet(v.stream(), ZNode::model);
                assertEquals(cachedModels, Sets.newHashSet(child1, child2));

                // verify that the same nodes are returned from the uncached method
                complete(ModeledFramework.wrap(async, modelSpec).child("p").childrenAsZNodes(), (v2, e2) -> {
                    Set<TestModel> uncachedModels = toSet(v2.stream(), ZNode::model);
                    assertEquals(cachedModels, uncachedModels);
                });
            });

            complete(
                    client.child("p").child("c1").childrenAsZNodes(),
                    (v, e) -> assertEquals(toSet(v.stream(), ZNode::model), Sets.newHashSet(grandChild1)));
            complete(
                    client.child("p").child("c2").childrenAsZNodes(),
                    (v, e) -> assertEquals(toSet(v.stream(), ZNode::model), Sets.newHashSet(grandChild2)));
        }
    }

    // note: CURATOR-546
    @ParameterizedTest
    @EnumSource(CachedModeledFrameworkType.class)
    void testAccessCacheDirectly(CachedModeledFrameworkType type) {
        TestModel model = new TestModel("a", "b", "c", 20, BigInteger.ONE);
        try (CachedModeledFramework<TestModel> client = build(type, modelSpec)) {
            CountDownLatch latch = new CountDownLatch(1);
            client.listenable().addListener((t, p, s, m) -> latch.countDown());

            client.start();
            complete(client.child("m").set(model));
            assertTrue(timing.awaitLatch(latch));

            // call 2 times in a row to validate CURATOR-546
            Optional<ZNode<TestModel>> optZNode =
                    client.cache().currentData(modelSpec.path().child("m"));
            assertEquals(
                    optZNode.orElseThrow(() -> new AssertionError("node is missing"))
                            .model(),
                    model);
            optZNode = client.cache().currentData(modelSpec.path().child("m"));
            assertEquals(
                    optZNode.orElseThrow(() -> new AssertionError("node is missing"))
                            .model(),
                    model);
        }
    }

    // Verify the CachedModeledFramework does not attempt to deserialize empty ZNodes on deletion using the Jackson
    // model serializer.
    // See: CURATOR-609
    @ParameterizedTest
    @EnumSource(CachedModeledFrameworkType.class)
    void testEmptyNodeJacksonDeserialization(CachedModeledFrameworkType type) {
        final TestModel model = new TestModel("a", "b", "c", 20, BigInteger.ONE);
        verifyEmptyNodeDeserialization(model, modelSpec, type);
    }

    // Verify the CachedModeledFramework does not attempt to deserialize empty ZNodes on deletion using the raw
    // model serializer.
    // See: CURATOR-609
    @ParameterizedTest
    @EnumSource(CachedModeledFrameworkType.class)
    void testEmptyNodeRawDeserialization(CachedModeledFrameworkType type) {
        final byte[] byteModel = {0x01, 0x02, 0x03};
        final ModelSpec<byte[]> byteModelSpec =
                ModelSpec.builder(path, ModelSerializer.raw).build();
        verifyEmptyNodeDeserialization(byteModel, byteModelSpec, type);
    }

    private <T> void verifyEmptyNodeDeserialization(
            T model, ModelSpec<T> parentModelSpec, CachedModeledFrameworkType type) {
        // The sub-path is the ZNode that will be removed that does not contain any model data. Their should be no
        // attempt to deserialize this empty ZNode.
        final String subPath = parentModelSpec.path().toString() + "/sub";

        final String testModelPath = subPath + "/test";
        final String signalModelPath = subPath + "/signal";

        final CountDownLatch latch = new CountDownLatch(1);

        final AtomicBoolean modelWasNull = new AtomicBoolean(false);
        final AtomicReference<Exception> caughtHandleException = new AtomicReference<>(null);

        // Create a custom listener to signal the end of the test and ensure that nothing is thrown.
        final ModeledCacheListener<T> listener = new ModeledCacheListener<T>() {
            @Override
            public void accept(Type t, ZPath p, Stat s, T m) {
                // We don't expect the handler to be called with a null model.
                if (m == null) {
                    modelWasNull.set(true);
                }

                if (t == ModeledCacheListener.Type.NODE_ADDED && p.toString().equals(signalModelPath)) {
                    latch.countDown();
                }
            }

            public void handleException(Exception e) {
                caughtHandleException.set(e);
            }
        };

        final ModelSerializer<T> serializer = parentModelSpec.serializer();

        // Create a cache client to watch the parent path.
        try (CachedModeledFramework<T> cacheClient = build(type, parentModelSpec)) {
            cacheClient.listenable().addListener(listener);

            ModelSpec<T> testModelSpec =
                    ModelSpec.builder(ZPath.parse(testModelPath), serializer).build();
            ModeledFramework<T> testModelClient = ModeledFramework.wrap(async, testModelSpec);

            ModelSpec<T> signalModelSpec =
                    ModelSpec.builder(ZPath.parse(signalModelPath), serializer).build();
            ModeledFramework<T> signalModelClient = ModeledFramework.wrap(async, signalModelSpec);

            cacheClient.start();

            // Create the test model (with sub-path), creating the initial parent path structure.
            complete(testModelClient.set(model));

            // Delete the test model, then delete the sub-path. As the sub-path ZNode is empty, we expect that this
            // should not throw an exception.
            complete(testModelClient.delete());
            complete(testModelClient.unwrap().delete().forPath(subPath));

            // Finally, create the signal model purely to signal the end of the test.
            complete(signalModelClient.set(model));

            assertTrue(timing.awaitLatch(latch));

            assertFalse(modelWasNull.get(), "Listener should never be called with a null model");
            assertNull(caughtHandleException.get(), "Exception should not have been handled by listener");
        }
    }

    @Test
    void testInitializedCachedModeledFramework() throws ExecutionException, InterruptedException, TimeoutException {
        try (CachedModeledFramework<TestModel> client = build(CachedModeledFrameworkType.INITIALIZED, modelSpec)) {
            TestModel model = new TestModel("a", "b", "c", 1, BigInteger.ONE);
            client.set(model);
            AsyncStage<TestModel> asyncModel = client.read();
            client.start();
            assertEquals(model, timing.getFuture(asyncModel.toCompletableFuture()));
        }
    }

    private <T, R> Set<R> toSet(Stream<T> stream, Function<? super T, ? extends R> mapper) {
        return stream.map(mapper).collect(Collectors.toSet());
    }

    private <T> CachedModeledFramework<T> build(CachedModeledFrameworkType type, ModelSpec<T> modelSpec) {
        if (type == CachedModeledFrameworkType.INITIALIZED) {
            return ModeledFramework.wrap(async, modelSpec).initialized();
        } else {
            return ModeledFramework.wrap(async, modelSpec).cached();
        }
    }
}
