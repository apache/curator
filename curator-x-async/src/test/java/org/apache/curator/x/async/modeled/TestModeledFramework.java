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
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.AsyncStage;
import org.apache.curator.x.async.modeled.models.TestModel;
import org.apache.curator.x.async.modeled.models.TestNewerModel;
import org.apache.curator.x.async.modeled.versioned.Versioned;
import org.apache.curator.x.async.modeled.versioned.VersionedModeledFramework;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

public class TestModeledFramework extends TestModeledFrameworkBase
{
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
        complete(client.child("one").set(model));
        complete(client.child("two").set(model));
        complete(client.child("three").set(model));

        Set<ZPath> expected = Sets.newHashSet(path.child("one"), path.child("two"), path.child("three"));
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

    @Test
    public void testVersioned()
    {
        ModeledFramework<TestModel> client = ModeledFramework.wrap(async, modelSpec);
        client.set(new TestModel("John", "Galt", "Galt's Gulch", 21, BigInteger.valueOf(1010101)));

        VersionedModeledFramework<TestModel> versioned = client.versioned();
        complete(versioned.read().whenComplete((v, e) -> {
            Assert.assertNull(e);
            Assert.assertTrue(v.version() > 0);
        }).thenCompose(versioned::set).whenComplete((s, e) -> Assert.assertNull(e))); // version is correct should succeed

        complete(versioned.read().whenComplete((v, e) -> {
            Assert.assertNull(e);
            Assert.assertTrue(v.version() > 0);
        }).thenCompose(value -> {
            Versioned<TestModel> badVersion = Versioned.from(value.model(), Integer.MAX_VALUE);
            return versioned.set(badVersion);
        }).whenComplete((s, e) -> Assert.assertTrue(e instanceof KeeperException.BadVersionException)));
    }

    @Test
    public void testAcl() throws NoSuchAlgorithmException
    {
        List<ACL> aclList = Collections.singletonList(new ACL(ZooDefs.Perms.WRITE, new Id("digest", DigestAuthenticationProvider.generateDigest("test:test"))));
        ModelSpec<TestModel> aclModelSpec = ModelSpec.builder(modelSpec.path(), modelSpec.serializer()).withAclList(aclList).build();
        ModeledFramework<TestModel> client = ModeledFramework.wrap(async, aclModelSpec);
        complete(client.set(new TestModel("John", "Galt", "Galt's Gulch", 21, BigInteger.valueOf(1010101))));
        complete(client.update(new TestModel("John", "Galt", "Galt's Gulch", 54, BigInteger.valueOf(88))), (__, e) -> Assert.assertNotNull(e, "Should've gotten an auth failure"));

        try ( CuratorFramework authCurator = CuratorFrameworkFactory.builder()
            .connectString(server.getConnectString())
            .retryPolicy(new RetryOneTime(1))
            .authorization("digest", "test:test".getBytes())
            .build() )
        {
            authCurator.start();
            ModeledFramework<TestModel> authClient = ModeledFramework.wrap(AsyncCuratorFramework.wrap(authCurator), aclModelSpec);
            complete(authClient.update(new TestModel("John", "Galt", "Galt's Gulch", 42, BigInteger.valueOf(66))), (__, e) -> Assert.assertNull(e, "Should've succeeded"));
        }
    }
}
