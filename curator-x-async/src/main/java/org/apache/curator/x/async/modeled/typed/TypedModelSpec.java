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
package org.apache.curator.x.async.modeled.typed;

import org.apache.curator.x.async.modeled.ModelSpec;
import org.apache.curator.x.async.modeled.ModelSpecBuilder;

/**
 * <p>
 *     Abstraction that allows the construction of ModelSpecs using strongly typed parameter replacements.
 *     For example, given a ModelSpec with a path such as "/root/registry/people/{id}" where "id" should
 *     be <code>PersonId</code>.
 * </p>
 *
 * <p>
 * <pre><code>
 * // Step 1. Create a typed ZPath
 * TypedZPath&lt;PersonId&gt; typedPath = TypedZPath.from("/root/registry/people/{id}");
 *
 * // Step 2. Create a ModelSpec builder (do not build at this point)
 * ModelSpecBuilder&lt;Person&gt; builder = ModelSpec.builder(JacksonModelSerializer.build(Person.class))
 *
 * // Step 3. Create a typed ModelSpec using the typed ZPath and ModelSpec builder
 * TypedModelSpec&lt;Person, PersonId&gt; typedModelSpec = TypedModelSpec.from(builder, path);
 *
 * // later on the TypedModelSpec can be resolved into a useable ModelSpec
 * ModelSpec&lt;Person&gt; modelSpec = typedModelSpec.resolve(personId);
 * </pre></code>
 * </p>
 */
@FunctionalInterface
public interface TypedModelSpec<M, P1>
{
    /**
     * Resolve into a ZPath using the given parameter
     *
     * @param p1 the parameter
     * @return ZPath
     */
    ModelSpec<M> resolved(P1 p1);

    /**
     * Return a new TypedModelSpec using the given model spec builder and typed path. When
     * {@link #resolved(Object)} is called the actual model spec is generated with the
     * resolved path
     *
     * @param builder model spec builder
     * @param path typed path
     * @return new TypedModelSpec
     */
    static <M, P1> TypedModelSpec<M, P1> from(ModelSpecBuilder<M> builder, TypedZPath<P1> path)
    {
        return p1 -> builder.withPath(path.resolved(p1)).build();
    }

    /**
     * Return a new TypedModelSpec using the given model spec builder and path. A TypedZPath
     * is created from the given full path and When
     * {@link #resolved(Object)} is called the actual model spec is generated with the
     * resolved path
     *
     * @param builder model spec builder
     * @param path typed path
     * @return new TypedModelSpec
     */
    static <M, P1> TypedModelSpec<M, P1> from(ModelSpecBuilder<M> builder, String path)
    {
        TypedZPath<P1> zPath = TypedZPath.from(path);
        return p1 -> builder.withPath(zPath.resolved(p1)).build();
    }
}
