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

import org.apache.curator.x.async.modeled.ZPath;

/**
 * <p>
 *     Abstraction that allows the construction of ZPaths using strongly typed parameter replacements.
 *     For example, given a path such as "/root/registry/people/{id}" where "id" should be <code>PersonId</code>.
 * </p>
 *
 * <p>
 * <pre><code>
 * TypedZPath&lt;PersonId&gt; typedPath = TypedZPath.from("/root/registry/people/{id}");
 *
 * ...
 *
 * ZPath path = typedPath.resolved(personId);
 * </pre></code>
 * </p>
 *
 * <p>
 *     Additionally, if you have a model/class that implements {@link org.apache.curator.x.async.modeled.NodeName}
 *     you can pass that when resolving. E.g.
 * </p>
 *
 * <p>
 * <pre><code>
 * public class MyModel implements NodeName {
 *     ...
 *     public String nodeName() {
 *         return modelId;
 *     }
 * }
 *
 * TypedZPath&lt;MyModel&gt; typedPath = TypedZPath.from("/foo/bar/{id}");
 *
 * MyModel model = ...
 * ZPath path = typedPath.resolved(model);
 * </pre></code>
 * </p>
 */
@FunctionalInterface
public interface TypedZPath<T>
{
    /**
     * Resolve into a ZPath using the given parameter
     *
     * @param p1 the parameter
     * @return ZPath
     */
    ZPath resolved(T p1);

    /**
     * Return a TypedZPath using {@link org.apache.curator.x.async.modeled.ZPath#parseWithIds}
     *
     * @param pathWithIds path to pass to {@link org.apache.curator.x.async.modeled.ZPath#parseWithIds}
     * @return TypedZPath
     */
    static <T> TypedZPath<T> from(String pathWithIds)
    {
        return from(ZPath.parseWithIds(pathWithIds));
    }

    /**
     * Return a TypedZPath
     *
     * @param path path to use
     * @return TypedZPath
     */
    static <T> TypedZPath<T> from(ZPath path)
    {
        return path::resolved;
    }
}
