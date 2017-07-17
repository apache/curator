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
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.WatchMode;
import org.apache.curator.x.async.modeled.details.ModeledFrameworkImpl;
import org.apache.zookeeper.WatchedEvent;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.function.UnaryOperator;

public class ModeledFrameworkBuilder<T>
{
    private AsyncCuratorFramework client;
    private ModelSpec<T> modelSpec;
    private WatchMode watchMode;
    private UnaryOperator<WatchedEvent> watcherFilter;
    private UnhandledErrorListener unhandledErrorListener;
    private UnaryOperator<CuratorEvent> resultFilter;
    private Set<ModeledOptions> modeledOptions;

    /**
     * Build a new ModeledFramework instance
     *
     * @return new ModeledFramework instance
     */
    public ModeledFramework<T> build()
    {
        return ModeledFrameworkImpl.build(
            client,
            modelSpec,
            watchMode,
            watcherFilter,
            unhandledErrorListener,
            resultFilter,
            modeledOptions
        );
    }

    /**
     * Add watchers as appropriate to the Modeled Curator's ZNode using
     * {@link org.apache.curator.x.async.WatchMode#stateChangeAndSuccess}
     *
     * @return this for chaining
     * @see org.apache.curator.x.async.AsyncStage#event()
     */
    public ModeledFrameworkBuilder<T> watched()
    {
        this.watchMode = WatchMode.stateChangeAndSuccess;
        return this;
    }

    /**
     * Add watchers as appropriate using the given watchMode to the Modeled Curator's ZNode
     *
     * @param watchMode watcher style
     * @return this for chaining
     * @see org.apache.curator.x.async.AsyncStage#event()
     */
    public ModeledFrameworkBuilder<T> watched(WatchMode watchMode)
    {
        this.watchMode = watchMode;
        return this;
    }

    /**
     * Add watchers as appropriate using the given watchMode and filter to the Modeled Curator's ZNode
     *
     * @param watchMode watcher style
     * @param watcherFilter filter
     * @return this for chaining
     * @see org.apache.curator.x.async.AsyncStage#event()
     */
    public ModeledFrameworkBuilder<T> watched(WatchMode watchMode, UnaryOperator<WatchedEvent> watcherFilter)
    {
        this.watchMode = watchMode;
        this.watcherFilter = watcherFilter;
        return this;
    }

    /**
     * Use the given unhandledErrorListener for operations on the Modeled Curator's ZNode
     *
     * @param unhandledErrorListener listener
     * @return this for chaining
     */
    public ModeledFrameworkBuilder<T> withUnhandledErrorListener(UnhandledErrorListener unhandledErrorListener)
    {
        this.unhandledErrorListener = unhandledErrorListener;
        return this;
    }

    /**
     * Use the given result filter for operations on the Modeled Curator's ZNode
     *
     * @param resultFilter filter
     * @return this for chaining
     */
    public ModeledFrameworkBuilder<T> withResultFilter(UnaryOperator<CuratorEvent> resultFilter)
    {
        this.resultFilter = resultFilter;
        return this;
    }

    /**
     * Change the model spec to use
     *
     * @param modelSpec model spec
     * @return this for chaining
     */
    public ModeledFrameworkBuilder<T> withModelSpec(ModelSpec<T> modelSpec)
    {
        this.modelSpec = Objects.requireNonNull(modelSpec, "modelSpec cannot be null");
        return this;
    }

    /**
     * Change the client to use
     *
     * @param client new client
     * @return this for chaining
     */
    public ModeledFrameworkBuilder<T> withClient(AsyncCuratorFramework client)
    {
        this.client = Objects.requireNonNull(client, "client cannot be null");
        return this;
    }

    /**
     * Change the modeled options
     *
     * @param modeledOptions new options set
     * @return this for chaining
     */
    public ModeledFrameworkBuilder<T> withOptions(Set<ModeledOptions> modeledOptions)
    {
        this.modeledOptions = ImmutableSet.copyOf(Objects.requireNonNull(modeledOptions, "client cannot be null"));
        return this;
    }

    ModeledFrameworkBuilder()
    {
    }

    ModeledFrameworkBuilder(AsyncCuratorFramework client, ModelSpec<T> modelSpec)
    {
        this.client = Objects.requireNonNull(client, "client cannot be null");
        this.modelSpec = Objects.requireNonNull(modelSpec, "modelSpec cannot be null");
        modeledOptions = Collections.singleton(ModeledOptions.ignoreMissingNodesForChildren);
    }
}
