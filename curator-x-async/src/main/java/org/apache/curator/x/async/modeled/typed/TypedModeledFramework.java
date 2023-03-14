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
 * <p>
 *     Abstraction that allows the construction of ModeledFrameworks using strongly typed parameter replacements.
 *     For example, given a ModeledFramework with a ModelSpec that has a path such as
 *     "/root/registry/people/{id}" where "id" should be <code>PersonId</code>.
 * </p>
 *
 * <p>
 * <pre><code>
 * // Step 1. Create a typed ZPath
 * TypedZPath&lt;PersonId&gt; typedPath = TypedZPath.from("/root/registry/people/{id}");
 *
 * // Step 2. Create a typed ModelSpec (see TypedModelSpec for details)
 * TypedModelSpec&lt;Person, PersonId&gt; typedModelSpec = TypedModelSpec.from(builder, path);
 *
 * // Step 3. Create a ModeledFramework builder (do not build at this point)
 * ModeledFrameworkBuilder&lt;Person&gt; builder = ModeledFramework.builder()... // add any other needed options
 *
 * // Step 4. Create a typed TypedModeledFramework using the typed ZPath, typed ModelSpec, and ModeledFramework builder
 * TypedModeledFramework&lt;Person, PersonId&gt; clientSpec = TypedModeledFramework.from(builder, modelSpec);
 *
 * // later on the TypedModelSpec can be resolved into a useable ModeledFramework
 * ModeledFramework&lt;Person&gt; client = clientSpec.resolve(personId);
 * </pre></code>
 * </p>
 */
@FunctionalInterface
public interface TypedModeledFramework<M, P1>
{
    /**
     * Resolve into a ModeledFramework using the given parameter
     *
     * @param client the curator instance to use
     * @param p1 the parameter
     * @return ZPath
     */
    ModeledFramework<M> resolved(AsyncCuratorFramework client, P1 p1);

    /**
     * Return a new TypedModeledFramework using the given modeled framework builder and typed model spec.
     * When {@link #resolved(AsyncCuratorFramework, Object)} is called the actual ModeledFramework is generated with the
     * resolved model spec
     *
     * @param frameworkBuilder ModeledFrameworkBuilder
     * @param modelSpec TypedModelSpec
     * @return new TypedModeledFramework
     */
    static <M, P1> TypedModeledFramework<M, P1> from(ModeledFrameworkBuilder<M> frameworkBuilder, TypedModelSpec<M, P1> modelSpec)
    {
        return (client, p1) -> frameworkBuilder.withClient(client).withModelSpec(modelSpec.resolved(p1)).build();
    }

    /**
     * Return a new TypedModeledFramework using the given modeled framework builder, model spec builder and a path with ids.
     * When {@link #resolved(AsyncCuratorFramework, Object)} is called the actual ModeledFramework is generated with the
     * resolved model spec and resolved path
     *
     * @param frameworkBuilder ModeledFrameworkBuilder
     * @param modelSpecBuilder model spec builder
     * @param pathWithIds path with {XXXX} parameters
     * @return new TypedModeledFramework
     */
    static <M, P1> TypedModeledFramework<M, P1> from(ModeledFrameworkBuilder<M> frameworkBuilder, ModelSpecBuilder<M> modelSpecBuilder, String pathWithIds)
    {
        TypedModelSpec<M, P1> typedModelSpec = TypedModelSpec.from(modelSpecBuilder, pathWithIds);
        return (client, p1) -> frameworkBuilder.withClient(client).withModelSpec(typedModelSpec.resolved(p1)).build();
    }
}
