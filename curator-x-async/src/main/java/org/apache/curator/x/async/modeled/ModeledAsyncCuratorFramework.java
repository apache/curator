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

import com.google.common.collect.ImmutableSet;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.x.async.AsyncStage;
import org.apache.curator.x.async.api.CreateOption;
import org.apache.curator.x.async.api.DeleteOption;
import org.apache.zookeeper.data.Stat;

public interface ModeledAsyncCuratorFramework<T>
{
    ImmutableSet<CreateOption> defaultCreateOptions = ImmutableSet.of(CreateOption.createParentsAsContainers, CreateOption.setDataIfExists);
    ImmutableSet<DeleteOption> defaultDeleteOptions = ImmutableSet.of(DeleteOption.guaranteed);

    /**
     * Return a new ModeledAsyncCuratorFramework for the given path and serializer. The returned ModeledAsyncCuratorFramework
     * is set to not watch ZNodes and uses {@link #defaultCreateOptions} and {@link #defaultDeleteOptions}.
     *
     * @param client Curator client
     * @param path path to model
     * @param serializer the model's serializer
     * @return Modeled Curator instance
     */
    static <T> ModeledAsyncCuratorFramework<T> wrap(CuratorFramework client, ZPath path, ModelSerializer<T> serializer)
    {
        return builder(client, path, serializer).build();
    }

    /**
     * Start a new ModeledAsyncCuratorFrameworkBuilder for the given path and serializer. The returned ModeledAsyncCuratorFrameworkBuilder
     * is set to not watch ZNodes and uses {@link #defaultCreateOptions} and {@link #defaultDeleteOptions}, but you can change these
     * with builder methods.
     *
     * @param client Curator client
     * @param path path to model
     * @param serializer the model's serializer
     * @return builder
     */
    static <T> ModeledAsyncCuratorFrameworkBuilder<T> builder(CuratorFramework client, ZPath path, ModelSerializer<T> serializer)
    {
        return new ModeledAsyncCuratorFrameworkBuilder<>(client, path, serializer)
            .withCreateOptions(defaultCreateOptions)
            .withDeleteOptions(defaultDeleteOptions);
    }

    CuratorFramework unwrap();

    ModeledAsyncCuratorFramework<T> at(String child);

    AsyncStage<String> create(T model);

    AsyncStage<String> create(T model, Stat storingStatIn);

    AsyncStage<T> read();

    AsyncStage<T> read(Stat storingStatIn);

    AsyncStage<Stat> update(T model);

    AsyncStage<Stat> update(T model, int version);

    AsyncStage<Stat> checkExists();

    AsyncStage<Void> delete();

    AsyncStage<Void> delete(int version);
}
