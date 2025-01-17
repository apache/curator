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

import java.util.Collections;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.CompletableBaseClassForTests;
import org.apache.curator.x.async.api.CreateOption;
import org.apache.curator.x.async.modeled.models.TestModel;
import org.apache.curator.x.async.modeled.models.TestNewerModel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public class TestModeledFrameworkBase extends CompletableBaseClassForTests {
    protected static final ZPath path = ZPath.parse("/test/path");
    protected CuratorFramework rawClient;
    protected ModelSpec<TestModel> modelSpec;
    protected ModelSpec<TestNewerModel> newModelSpec;
    protected ModelSpec<TestModel> compressedModelSpec;
    protected ModelSpec<TestModel> uncompressedModelSpec;
    protected AsyncCuratorFramework async;

    public CuratorFrameworkFactory.Builder createRawClientBuilder() {
        return CuratorFrameworkFactory.builder()
                .connectString(server.getConnectString())
                .retryPolicy(new RetryOneTime(1))
                .sessionTimeoutMs(timing.session())
                .connectionTimeoutMs(timing.connection());
    }

    @BeforeEach
    @Override
    public void setup() throws Exception {
        super.setup();

        rawClient = createRawClientBuilder().build();
        rawClient.start();
        async = AsyncCuratorFramework.wrap(rawClient);

        JacksonModelSerializer<TestModel> serializer = JacksonModelSerializer.build(TestModel.class);
        JacksonModelSerializer<TestNewerModel> newSerializer = JacksonModelSerializer.build(TestNewerModel.class);

        modelSpec = ModelSpec.builder(path, serializer).build();
        newModelSpec = ModelSpec.builder(path, newSerializer).build();
        compressedModelSpec = ModelSpec.builder(path, serializer)
                .withCreateOptions(Collections.singleton(CreateOption.compress))
                .build();
        uncompressedModelSpec = ModelSpec.builder(path, serializer)
                .withCreateOptions(Collections.singleton(CreateOption.uncompress))
                .build();
    }

    @AfterEach
    @Override
    public void teardown() throws Exception {
        CloseableUtils.closeQuietly(rawClient);
        super.teardown();
    }
}
