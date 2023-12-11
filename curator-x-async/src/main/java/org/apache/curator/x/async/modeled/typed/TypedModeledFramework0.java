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

package org.apache.curator.x.async.modeled.typed;

import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.modeled.ModelSpecBuilder;
import org.apache.curator.x.async.modeled.ModeledFramework;
import org.apache.curator.x.async.modeled.ModeledFrameworkBuilder;

/**
 * Same as {@link TypedModeledFramework}, but with 0 parameters
 */
@FunctionalInterface
public interface TypedModeledFramework0<M> {
    ModeledFramework<M> resolved(AsyncCuratorFramework client);

    /**
     * Return a new TypedModeledFramework using the given modeled framework builder and typed model spec.
     * When {@link #resolved(org.apache.curator.x.async.AsyncCuratorFramework)} is called the actual ModeledFramework is generated with the
     * resolved model spec
     *
     * @param frameworkBuilder ModeledFrameworkBuilder
     * @param modelSpec TypedModelSpec
     * @return new TypedModeledFramework
     */
    static <M> TypedModeledFramework0<M> from(
            ModeledFrameworkBuilder<M> frameworkBuilder, TypedModelSpec0<M> modelSpec) {
        return (client) -> frameworkBuilder
                .withClient(client)
                .withModelSpec(modelSpec.resolved())
                .build();
    }

    /**
     * Return a new TypedModeledFramework using the given modeled framework builder, model spec builder and a path with ids.
     * When {@link #resolved(org.apache.curator.x.async.AsyncCuratorFramework)} is called the actual ModeledFramework is generated with the
     * resolved model spec and resolved path
     *
     * @param frameworkBuilder ModeledFrameworkBuilder
     * @param modelSpecBuilder model spec builder
     * @param pathWithIds path with {XXXX} parameters
     * @return new TypedModeledFramework
     */
    static <M> TypedModeledFramework0<M> from(
            ModeledFrameworkBuilder<M> frameworkBuilder, ModelSpecBuilder<M> modelSpecBuilder, String pathWithIds) {
        TypedModelSpec0<M> typedModelSpec = TypedModelSpec0.from(modelSpecBuilder, pathWithIds);
        return (client) -> frameworkBuilder
                .withClient(client)
                .withModelSpec(typedModelSpec.resolved())
                .build();
    }
}
