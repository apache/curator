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

import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.AsyncStage;
import org.apache.curator.x.async.modeled.cached.CachedModeledFramework;
import org.apache.curator.x.async.modeled.versioned.VersionedModeledFramework;
import org.apache.zookeeper.data.Stat;
import java.util.List;
import java.util.concurrent.ExecutorService;

public interface ModeledFramework<T>
{
    /**
     * Return a new ModeledFramework for the given model
     *
     * @param client Curator client
     * @param model the model
     * @return new Modeled Curator instance
     */
    static <T> ModeledFramework<T> wrap(AsyncCuratorFramework client, ModelSpec<T> model)
    {
        return builder(client, model).build();
    }

    /**
     * Start a new ModeledFrameworkBuilder for the given model
     *
     * @param client Curator client
     * @param model the model
     * @return builder
     */
    static <T> ModeledFrameworkBuilder<T> builder(AsyncCuratorFramework client, ModelSpec<T> model)
    {
        return new ModeledFrameworkBuilder<>(client, model);
    }

    /**
     * Start a new ModeledFrameworkBuilder. A client and model must be provided prior to the instance
     * being built via {@link org.apache.curator.x.async.modeled.ModeledFrameworkBuilder#withClient(org.apache.curator.x.async.AsyncCuratorFramework)}
     * and {@link org.apache.curator.x.async.modeled.ModeledFrameworkBuilder#withModelSpec(ModelSpec)}
     *
     * @return builder
     */
    static <T> ModeledFrameworkBuilder<T> builder()
    {
        return new ModeledFrameworkBuilder<>();
    }

    /**
     * <p>
     *     Use an internally created cache as a front for this modeled instance. All read APIs use the internal
     *     cache. i.e. read calls always use the cache instead of making direct queries. Note: you must call
     *     {@link org.apache.curator.x.async.modeled.cached.CachedModeledFramework#start()} and
     *     {@link org.apache.curator.x.async.modeled.cached.CachedModeledFramework#close()} to start/stop
     * </p>
     *
     * <p>
     *     Note: this method internally allocates an Executor for the cache and read methods. Use
     *     {@link #cached(java.util.concurrent.ExecutorService)} if you'd like to provide your own executor service.
     * </p>
     *
     * @return wrapped instance
     */
    CachedModeledFramework<T> cached();

    /**
     * Same as {@link #cached()} but allows for providing an executor service
     *
     * @param executor thread pool to use for the cache and for read operations
     * @return wrapped instance
     */
    CachedModeledFramework<T> cached(ExecutorService executor);

    /**
     * Return mutator APIs that work with {@link org.apache.curator.x.async.modeled.versioned.Versioned} containers
     *
     * @return wrapped instance
     */
    VersionedModeledFramework<T> versioned();

    /**
     * Returns the client that was originally passed to {@link #wrap(org.apache.curator.x.async.AsyncCuratorFramework, ModelSpec)} or
     * the builder.
     *
     * @return original client
     */
    AsyncCuratorFramework unwrap();

    /**
     * Return the model being used
     *
     * @return model
     */
    ModelSpec<T> modelSpec();

    /**
     * <p>
     *     Return a new Modeled Curator instance with all the same options but applying to the given child node of this Modeled Curator's
     *     path. E.g. if this Modeled Curator instance applies to "/a/b", calling <code>modeled.at("c")</code> returns an instance that applies to
     *     "/a/b/c".
     * </p>
     *
     * <p>
     *     The replacement is the <code>toString()</code> value of child or,
     *     if it implements {@link org.apache.curator.x.async.modeled.NodeName},
     *     the value of <code>nodeName()</code>.
     * </p>
     *
     * @param child child node.
     * @return new Modeled Curator instance
     */
    ModeledFramework<T> child(Object child);

    /**
     * <p>
     *     Return a new Modeled Curator instance with all the same options but applying to the parent node of this Modeled Curator's
     *     path. E.g. if this Modeled Curator instance applies to "/a/b/c", calling <code>modeled.parent()</code> returns an instance that applies to
     *     "/a/b".
     * </p>
     *
     * <p>
     *     The replacement is the <code>toString()</code> value of child or,
     *     if it implements {@link org.apache.curator.x.async.modeled.NodeName},
     *     the value of <code>nodeName()</code>.
     * </p>
     *
     * @return new Modeled Curator instance
     */
    ModeledFramework<T> parent();

    /**
     * Return a Modeled Curator instance with all the same options but using the given path.
     *
     * @param path new path
     * @return new Modeled Curator instance
     */
    ModeledFramework<T> withPath(ZPath path);

    /**
     * Create (or update depending on build options) a ZNode at this instance's path with a serialized
     * version of the given model
     *
     * @param model model to write
     * @return AsyncStage
     * @see org.apache.curator.x.async.AsyncStage
     */
    AsyncStage<String> set(T model);

    /**
     * Create (or update depending on build options) a ZNode at this instance's path with a serialized
     * version of the given model
     *
     * @param model model to write
     * @param version if data is being set instead of creating the node, the data version to use
     * @return AsyncStage
     * @see org.apache.curator.x.async.AsyncStage
     */
    AsyncStage<String> set(T model, int version);

    /**
     * Create (or update depending on build options) a ZNode at this instance's path with a serialized
     * form of the given model
     *
     * @param model model to write
     * @param storingStatIn the stat for the new ZNode is stored here
     * @return AsyncStage
     * @see org.apache.curator.x.async.AsyncStage
     */
    AsyncStage<String> set(T model, Stat storingStatIn);

    /**
     * Create (or update depending on build options) a ZNode at this instance's path with a serialized
     * form of the given model
     *
     * @param model model to write
     * @param version if data is being set instead of creating the node, the data version to use
     * @param storingStatIn the stat for the new ZNode is stored here
     * @return AsyncStage
     * @see org.apache.curator.x.async.AsyncStage
     */
    AsyncStage<String> set(T model, Stat storingStatIn, int version);

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
     * Read the ZNode at this instance's path and deserialize into a model
     *
     * @return AsyncStage
     * @see org.apache.curator.x.async.AsyncStage
     */
    AsyncStage<ZNode<T>> readAsZNode();

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
     * Return the child paths of this instance's path (in no particular order)
     *
     * @return AsyncStage
     * @see org.apache.curator.x.async.AsyncStage
     */
    AsyncStage<List<ZPath>> children();

    /**
     * Return the child paths of this instance's parent path (in no particular order)
     *
     * @return AsyncStage
     * @see org.apache.curator.x.async.AsyncStage
     */
    AsyncStage<List<ZPath>> siblings();

    /**
     * Create operation instance that can be passed among other operations to
     * {@link #inTransaction(java.util.List)} to be executed as a single transaction. Note:
     * due to ZooKeeper transaction limits, this is a _not_ a "set or update" operation but only
     * a create operation and will generate an error if the node already exists.
     *
     * @param model the model
     * @return operation
     */
    CuratorOp createOp(T model);

    /**
     * Update operation instance that can be passed among other operations to
     * {@link #inTransaction(java.util.List)} to be executed as a single transaction.
     *
     * @param model the model
     * @return operation
     */
    CuratorOp updateOp(T model);

    /**
     * Create operation instance that can be passed among other operations to
     * {@link #inTransaction(java.util.List)} to be executed as a single transaction.
     *
     * @param model the model
     * @param version update version to use
     * @return operation
     */
    CuratorOp updateOp(T model, int version);

    /**
     * Delete operation instance that can be passed among other operations to
     * {@link #inTransaction(java.util.List)} to be executed as a single transaction.
     *
     * @return operation
     */
    CuratorOp deleteOp();

    /**
     * Delete operation instance that can be passed among other operations to
     * {@link #inTransaction(java.util.List)} to be executed as a single transaction.
     *
     * @param version delete version to use
     * @return operation
     */
    CuratorOp deleteOp(int version);

    /**
     * Check exists operation instance that can be passed among other operations to
     * {@link #inTransaction(java.util.List)} to be executed as a single transaction.
     *
     * @return operation
     */
    CuratorOp checkExistsOp();

    /**
     * Check exists operation instance that can be passed among other operations to
     * {@link #inTransaction(java.util.List)} to be executed as a single transaction.
     *
     * @param version version to use
     * @return operation
     */
    CuratorOp checkExistsOp(int version);

    /**
     * Invoke ZooKeeper to commit the given operations as a single transaction.
     *
     * @param operations operations that make up the transaction.
     * @return AsyncStage instance for managing the completion
     */
    AsyncStage<List<CuratorTransactionResult>> inTransaction(List<CuratorOp> operations);
}
