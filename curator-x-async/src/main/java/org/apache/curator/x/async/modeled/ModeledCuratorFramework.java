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
import org.apache.curator.x.async.modeled.caching.Caching;
import org.apache.zookeeper.data.Stat;
import java.util.List;
import java.util.Set;

public interface ModeledCuratorFramework<T>
{
    Set<CreateOption> defaultCreateOptions = ImmutableSet.of(CreateOption.createParentsAsContainers, CreateOption.setDataIfExists);
    Set<DeleteOption> defaultDeleteOptions = ImmutableSet.of(DeleteOption.guaranteed);

    /**
     * Return a new ModeledCuratorFramework for the given path and serializer. The returned ModeledCuratorFramework
     * is set to not watch ZNodes and uses {@link #defaultCreateOptions} and {@link #defaultDeleteOptions}.
     *
     * @param client Curator client
     * @param path path to model
     * @param serializer the model's serializer
     * @return new Modeled Curator instance
     */
    static <T> ModeledCuratorFramework<T> wrap(CuratorFramework client, ZPath path, ModelSerializer<T> serializer)
    {
        return builder(client, path, serializer).build();
    }

    /**
     * Start a new ModeledCuratorFrameworkBuilder for the given path and serializer. The returned ModeledCuratorFrameworkBuilder
     * is set to not watch ZNodes and uses {@link #defaultCreateOptions} and {@link #defaultDeleteOptions}, but you can change these
     * with builder methods.
     *
     * @param client Curator client
     * @param path path to model
     * @param serializer the model's serializer
     * @return builder
     */
    static <T> ModeledCuratorFrameworkBuilder<T> builder(CuratorFramework client, ZPath path, ModelSerializer<T> serializer)
    {
        return new ModeledCuratorFrameworkBuilder<>(client, path, serializer)
            .withCreateOptions(defaultCreateOptions)
            .withDeleteOptions(defaultDeleteOptions);
    }

    /**
     * Returns the client that was originally passed to {@link #wrap(org.apache.curator.framework.CuratorFramework, ZPath, ModelSerializer)} or
     * the builder.
     *
     * @return original client
     */
    CuratorFramework unwrap();

    /**
     * Return the caching APIs. Only valid if {@link ModeledCuratorFrameworkBuilder#cached()} or
     * {@link ModeledCuratorFrameworkBuilder#cached(java.util.Set)} was called when building the instance.
     *
     * @return caching APIs
     * @throws java.lang.IllegalStateException if caching was not enabled when building the instance
     */
    Caching<T> caching();

    /**
     * Return a new Modeled Curator instance with all the same options but applying to the given child node of this Modeled Curator's
     * path. E.g. if this Modeled Curator instance applies to "/a/b", calling <code>modeled.at("c")</code> returns an instance that applies to
     * "/a/b/c".
     *
     * @param child child node.
     * @return new Modeled Curator instance
     */
    ModeledCuratorFramework<T> at(String child);

    /**
     * Create (or update depending on build options) a ZNode at this instance's path with a serialized
     * version of the given model
     *
     * @param model model to write
     * @return AsyncStage
     * @see org.apache.curator.x.async.AsyncStage
     */
    AsyncStage<String> create(T model);

    /**
     * Create (or update depending on build options) a ZNode at this instance's path with a serialized
     * form of the given model
     *
     * @param model model to write
     * @param storingStatIn the stat for the new ZNode is stored here
     * @return AsyncStage
     * @see org.apache.curator.x.async.AsyncStage
     */
    AsyncStage<String> create(T model, Stat storingStatIn);

    /**
     * Read the ZNode at this instance's path and deserialize into a model
     *
     * @return AsyncStage
     * @see org.apache.curator.x.async.AsyncStage
     */
    AsyncStage<T> read();

    /**
     * Read the ZNode at this instance's path and deserialize into a model
     *
     * @param storingStatIn the stat for the new ZNode is stored here
     * @return AsyncStage
     * @see org.apache.curator.x.async.AsyncStage
     */
    AsyncStage<T> read(Stat storingStatIn);

    /**
     * Update the ZNode at this instance's path with a serialized
     * form of the given model passing "-1" for the update version
     *
     * @param model model to write
     * @return AsyncStage
     * @see org.apache.curator.x.async.AsyncStage
     */
    AsyncStage<Stat> update(T model);

    /**
     * Update the ZNode at this instance's path with a serialized
     * form of the given model passing the given update version
     *
     * @param model model to write
     * @param version update version to use
     * @return AsyncStage
     * @see org.apache.curator.x.async.AsyncStage
     */
    AsyncStage<Stat> update(T model, int version);

    /**
     * Delete the ZNode at this instance's path passing -1 for the delete version
     *
     * @return AsyncStage
     * @see org.apache.curator.x.async.AsyncStage
     */
    AsyncStage<Void> delete();

    /**
     * Delete the ZNode at this instance's path passing the given delete version
     *
     * @param version update version to use
     * @return AsyncStage
     * @see org.apache.curator.x.async.AsyncStage
     */
    AsyncStage<Void> delete(int version);

    /**
     * Check to see if the ZNode at this instance's path exists
     *
     * @return AsyncStage
     * @see org.apache.curator.x.async.AsyncStage
     */
    AsyncStage<Stat> checkExists();

    /**
     * Return the child paths of this instance's paths (in no particular order)
     *
     * @return AsyncStage
     * @see org.apache.curator.x.async.AsyncStage
     */
    AsyncStage<List<ZPath>> getChildren();
}
