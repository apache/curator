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

import org.apache.curator.x.async.modeled.NodeName;
import org.apache.curator.x.async.modeled.ZPath;

/**
 * Same as {@link org.apache.curator.x.async.modeled.typed.TypedZPath}, but with 9 parameters
 */
@FunctionalInterface
public interface TypedZPath9<T1, T2, T3, T4, T5, T6, T7, T8, T9>
{
    ZPath resolved(T1 p1, T2 p2, T3 p3, T4 p4, T5 p5, T6 p6, T7 p7, T8 p8, T9 p9);

    /**
     * Return a TypedZPath using {@link org.apache.curator.x.async.modeled.ZPath#parseWithIds}
     *
     * @param fullPath path to pass to {@link org.apache.curator.x.async.modeled.ZPath#parseWithIds}
     * @return TypedZPath
     */
    static <T1, T2, T3, T4, T5, T6, T7, T8, T9> TypedZPath9<T1, T2, T3, T4, T5, T6, T7, T8, T9> from(String fullPath)
    {
        return from(ZPath.parseWithIds(fullPath));
    }

    /**
     * Return a TypedZPath
     *
     * @param path path to use
     * @return TypedZPath
     */
    static <T1, T2, T3, T4, T5, T6, T7, T8, T9> TypedZPath9<T1, T2, T3, T4, T5, T6, T7, T8, T9> from(ZPath path)
    {
        return (p1, p2, p3, p4, p5, p6, p7, p8, p9) -> path.resolved(NodeName.nameFrom(p1), NodeName.nameFrom(p2), NodeName.nameFrom(p3), NodeName.nameFrom(p4), NodeName.nameFrom(p5), NodeName.nameFrom(p6), NodeName.nameFrom(p7), NodeName.nameFrom(p8), NodeName.nameFrom(p9));
    }
}
