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

import org.apache.curator.x.async.modeled.ModelSpec;
import org.apache.curator.x.async.modeled.ModelSpecBuilder;

/**
 * Same as {@link org.apache.curator.x.async.modeled.typed.TypedModelSpec}, but with 10 parameters
 */
@FunctionalInterface
public interface TypedModelSpec10<M, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10>
{
    ModelSpec<M> resolved(P1 p1, P2 p2, P3 p3, P4 p4, P5 p5, P6 p6, P7 p7, P8 p8, P9 p9, P10 p10);

    /**
     * Return a new TypedModelSpec using the given model spec builder and typed path. When
     * {@link #resolved(Object, Object, Object, Object, Object, Object, Object, Object, Object, Object)} is called the actual model spec is generated with the
     * resolved path
     *
     * @param builder model spec builder
     * @param path typed path
     * @return new TypedModelSpec
     */
    static <M, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10> TypedModelSpec10<M, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10> from(ModelSpecBuilder<M> builder, TypedZPath10<P1, P2, P3, P4, P5, P6, P7, P8, P9, P10> path)
    {
        return (p1, p2, p3, p4, p5, p6, p7, p8, p9, p10) -> builder.withPath(path.resolved(p1, p2, p3, p4, p5, p6, p7, p8, p9, p10)).build();
    }

    /**
     * Return a new TypedModelSpec using the given model spec builder and path. A TypedZPath
     * is created from the given full path and When
     * {@link #resolved(Object, Object, Object, Object, Object, Object, Object, Object, Object, Object)} is called the actual model spec is generated with the
     * resolved path
     *
     * @param builder model spec builder
     * @param pathWithIds typed path
     * @return new TypedModelSpec
     */
    static <M, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10> TypedModelSpec10<M, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10> from(ModelSpecBuilder<M> builder, String pathWithIds)
    {
        TypedZPath10<P1, P2, P3, P4, P5, P6, P7, P8, P9, P10> zPath = TypedZPath10.from(pathWithIds);
        return (p1, p2, p3, p4, p5, p6, p7, p8, p9, p10) -> builder.withPath(zPath.resolved(p1, p2, p3, p4, p5, p6, p7, p8, p9, p10)).build();
    }
}
