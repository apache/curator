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

import com.google.common.collect.Sets;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.schema.Schema;
import org.apache.curator.framework.schema.SchemaSet;
import org.apache.curator.framework.schema.SchemaViolation;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.AsyncStage;
import org.apache.curator.x.async.CompletableBaseClassForTests;
import org.apache.curator.x.async.modeled.models.TestModel;
import org.apache.curator.x.async.modeled.models.TestNewerModel;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.math.BigInteger;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

public class TestModeledFramework extends CompletableBaseClassForTests
{
    private static final ZPath path = ZPath.parse("/test/path");
    private CuratorFramework rawClient;
    private ModelSpec<TestModel> modelSpec;
    private ModelSpec<TestNewerModel> newModelSpec;
    private AsyncCuratorFramework async;

    @BeforeMethod
    @Override
    public void setup() throws Exception
    {
        super.setup();

        rawClient = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        rawClient.start();
        async = AsyncCuratorFramework.wrap(rawClient);

        JacksonModelSerializer<TestModel> serializer = JacksonModelSerializer.build(TestModel.class);
        JacksonModelSerializer<TestNewerModel> newSerializer = JacksonModelSerializer.build(TestNewerModel.class);

        modelSpec = ModelSpec.builder(path, serializer).build();
        newModelSpec = ModelSpec.builder(path, newSerializer).build();
    }

    @AfterMethod
    @Override
    public void teardown() throws Exception
    {
        CloseableUtils.closeQuietly(rawClient);
        super.teardown();
    }

    @Test
    public void testCrud()
    {
        TestModel rawModel = new TestModel("John", "Galt", "1 Galt's Gulch", 42, BigInteger.valueOf(1));
        TestModel rawModel2 = new TestModel("Wayne", "Rooney", "Old Trafford", 10, BigInteger.valueOf(1));
        ModeledFramework<TestModel> client = ModeledFramework.wrap(async, modelSpec);
        AsyncStage<String> stage = client.set(rawModel);
        Assert.assertNull(stage.event());
        complete(stage, (s, e) -> Assert.assertNotNull(s));
        complete(client.read(), (model, e) -> Assert.assertEquals(model, rawModel));
        complete(client.update(rawModel2));
        complete(client.read(), (model, e) -> Assert.assertEquals(model, rawModel2));
        complete(client.delete());
        complete(client.checkExists(), (stat, e) -> Assert.assertNull(stat));
    }

    @Test
    public void testBackwardCompatibility()
    {
        TestNewerModel rawNewModel = new TestNewerModel("John", "Galt", "1 Galt's Gulch", 42, BigInteger.valueOf(1), 100);
        ModeledFramework<TestNewerModel> clientForNew = ModeledFramework.wrap(async, newModelSpec);
        complete(clientForNew.set(rawNewModel), (s, e) -> Assert.assertNotNull(s));

        ModeledFramework<TestModel> clientForOld = ModeledFramework.wrap(async, modelSpec);
        complete(clientForOld.read(), (model, e) -> Assert.assertTrue(rawNewModel.equalsOld(model)));
    }

    @Test
    public void testWatched() throws InterruptedException
    {
        CountDownLatch latch = new CountDownLatch(1);
        ModeledFramework<TestModel> client = ModeledFramework.builder(async, modelSpec).watched().build();
        client.checkExists().event().whenComplete((event, ex) -> latch.countDown());
        timing.sleepABit();
        Assert.assertEquals(latch.getCount(), 1);
        client.set(new TestModel());
        Assert.assertTrue(timing.awaitLatch(latch));
    }

    @Test
    public void testGetChildren()
    {
        TestModel model = new TestModel("John", "Galt", "1 Galt's Gulch", 42, BigInteger.valueOf(1));
        ModeledFramework<TestModel> client = ModeledFramework.builder(async, modelSpec).build();
        complete(client.at("one").set(model));
        complete(client.at("two").set(model));
        complete(client.at("three").set(model));

        Set<ZPath> expected = Sets.newHashSet(path.at("one"), path.at("two"), path.at("three"));
        complete(client.children(), (children, e) -> Assert.assertEquals(Sets.newHashSet(children), expected));
    }

    @Test
    public void testBadNode()
    {
        complete(async.create().forPath(modelSpec.path().fullPath(), "fubar".getBytes()));

        ModeledFramework<TestModel> client = ModeledFramework.builder(async, modelSpec).watched().build();
        complete(client.read().whenComplete((model, e) -> Assert.assertTrue(e instanceof RuntimeException)));
    }

    @Test
    public void testSchema() throws Exception
    {
        Schema schema = modelSpec.schema();
        try ( CuratorFramework schemaClient = CuratorFrameworkFactory.builder()
            .connectString(server.getConnectString())
            .retryPolicy(new RetryOneTime(1))
            .schemaSet(new SchemaSet(Collections.singletonList(schema), false))
            .build() ) {
            schemaClient.start();

            try
            {
                schemaClient.create().forPath(modelSpec.path().fullPath(), "asflasfas".getBytes());
                Assert.fail("Should've thrown SchemaViolation");
            }
            catch ( SchemaViolation dummy )
            {
                // expected
            }

            ModeledFramework<TestModel> modeledSchemaClient = ModeledFramework.wrap(AsyncCuratorFramework.wrap(schemaClient), modelSpec);
            complete(modeledSchemaClient.set(new TestModel("one", "two", "three", 4, BigInteger.ONE)), (dummy, e) -> Assert.assertNull(e));
        }
    }
}
