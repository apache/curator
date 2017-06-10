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
 * Same as {@link TypedModelSpec}, but with 0 parameters
 */
@FunctionalInterface
public interface TypedModelSpec0<M>
{
    ModelSpec<M> resolved();

    /**
     * Return a new TypedModelSpec using the given model spec builder and typed path. When
     * {@link #resolved()} is called the actual model spec is generated with the
     * resolved path
     *
     * @param builder model spec builder
     * @param path typed path
     * @return new TypedModelSpec
     */
    static <M> TypedModelSpec0<M> from(ModelSpecBuilder<M> builder, TypedZPath0 path)
    {
        return () -> builder.withPath(path.resolved()).build();
    }

    /**
     * Return a new TypedModelSpec using the given model spec builder and path. A TypedZPath
     * is created from the given full path and When
     * {@link #resolved()} is called the actual model spec is generated with the
     * resolved path
     *
     * @param builder model spec builder
     * @param pathWithIds typed path
     * @return new TypedModelSpec
     */
    static <M> TypedModelSpec0<M> from(ModelSpecBuilder<M> builder, String pathWithIds)
    {
        TypedZPath0 zPath = TypedZPath0.from(pathWithIds);
        return () -> builder.withPath(zPath.resolved()).build();
    }
}
