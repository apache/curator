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

import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.modeled.ModelSpecBuilder;
import org.apache.curator.x.async.modeled.ModeledFramework;
import org.apache.curator.x.async.modeled.ModeledFrameworkBuilder;

/**
 * Same as {@link org.apache.curator.x.async.modeled.typed.TypedModeledFramework}, but with 6 parameters
 */
@FunctionalInterface
public interface TypedModeledFramework6<M, P1, P2, P3, P4, P5, P6>
{
    ModeledFramework<M> resolved(AsyncCuratorFramework client, P1 p1, P2 p2, P3 p3, P4 p4, P5 p5, P6 p6);

    /**
     * Return a new TypedModeledFramework using the given modeled framework builder and typed model spec.
     * When {@link #resolved(AsyncCuratorFramework, Object, Object, Object, Object, Object, Object)} is called the actual ModeledFramework is generated with the
     * resolved model spec
     *
     * @param frameworkBuilder ModeledFrameworkBuilder
     * @param modelSpec TypedModelSpec
     * @return new TypedModeledFramework
     */
    static <M, P1, P2, P3, P4, P5, P6> TypedModeledFramework6<M, P1, P2, P3, P4, P5, P6> from(ModeledFrameworkBuilder<M> frameworkBuilder, TypedModelSpec6<M, P1, P2, P3, P4, P5, P6> modelSpec)
    {
        return (client, p1, p2, p3, p4, p5, p6) -> frameworkBuilder.withClient(client).withModelSpec(modelSpec.resolved(p1, p2, p3, p4, p5, p6)).build();
    }

    /**
     * Return a new TypedModeledFramework using the given modeled framework builder, model spec builder and a path with ids.
     * When {@link #resolved(AsyncCuratorFramework, Object, Object, Object, Object, Object, Object)} is called the actual ModeledFramework is generated with the
     * resolved model spec and resolved path
     *
     * @param frameworkBuilder ModeledFrameworkBuilder
     * @param modelSpecBuilder model spec builder
     * @param pathWithIds path with {XXXX} parameters
     * @return new TypedModeledFramework
     */
    static <M, P1, P2, P3, P4, P5, P6> TypedModeledFramework6<M, P1, P2, P3, P4, P5, P6> from(ModeledFrameworkBuilder<M> frameworkBuilder, ModelSpecBuilder<M> modelSpecBuilder, String pathWithIds)
    {
        TypedModelSpec6<M, P1, P2, P3, P4, P5, P6> typedModelSpec = TypedModelSpec6.from(modelSpecBuilder, pathWithIds);
        return (client, p1, p2, p3, p4, p5, p6) -> frameworkBuilder.withClient(client).withModelSpec(typedModelSpec.resolved(p1, p2, p3, p4, p5, p6)).build();
    }
}
