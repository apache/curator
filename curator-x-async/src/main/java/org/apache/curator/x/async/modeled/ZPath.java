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

import org.apache.curator.x.async.modeled.details.ZPathImpl;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.regex.Pattern;

/**
 * Abstracts a ZooKeeper ZNode path
 */
public interface ZPath
{
    /**
     * Return the root path: "/"
     *
     * @return root path
     */
    static ZPath root()
    {
        return ZPathImpl.root;
    }

    /**
     * Take a ZNode string path and return a ZPath
     *
     * @param fullPath the path to parse
     * @return ZPath
     * @throws IllegalArgumentException if the path is invalid
     */
    static ZPath parse(String fullPath)
    {
        return ZPathImpl.parse(fullPath);
    }

    /**
     * Convert individual path names into a ZPath. E.g.
     * <code>ZPath.from("my", "full", "path")</code>
     *
     * @param names path names
     * @return ZPath
     * @throws IllegalArgumentException if any of the names is invalid
     */
    static ZPath from(String... names)
    {
        return ZPathImpl.from(names);
    }

    /**
     * Convert individual path names into a ZPath
     *
     * @param names path names
     * @return ZPath
     * @throws IllegalArgumentException if any of the names is invalid
     */
    static ZPath from(List<String> names)
    {
        return ZPathImpl.from(names);
    }

    /**
     * Return the special node name that can be used for replacements at runtime
     * via {@link #resolved(Object...)}
     *
     * @return name
     */
    static String parameterNodeName()
    {
        return ZPathImpl.parameter;
    }

    /**
     * When creating paths, any node in the path can be set to {@link #parameterNodeName()}.
     * At runtime, the ZPath can be "resolved" by replacing these nodes with values.
     *
     * @param parameters list of replacements. Must have be the same length as the number of
     *                   parameter nodes in the path
     * @return new resolved ZPath
     */
    default ZPath resolved(Object... parameters)
    {
        return resolved(Arrays.asList(parameters));
    }

    /**
     * When creating paths, any node in the path can be set to {@link #parameterNodeName()}.
     * At runtime, the ZPath can be "resolved" by replacing these nodes with values.
     *
     * @param parameters list of replacements. Must have be the same length as the number of
     *                   parameter nodes in the path
     * @return new resolved ZPath
     */
    ZPath resolved(List<Object> parameters);

    /**
     * An "auto" resolving version of this ZPath. i.e. if any of the path names is
     * the {@link #parameterNodeName()} the ZPath must be resolved. This method
     * creates a new ZPath that auto resolves by using the given parameter suppliers
     * whenever needed.
     *
     * @param parameterSuppliers parameter suppliers
     * @return new auto resolving ZNode
     * @see #resolved(Object...)
     * @see #parameterNodeName()
     */
    ZPath resolving(List<Supplier<Object>> parameterSuppliers);

    /**
     * Return a ZPath that represents a child ZNode of this ZPath. e.g.
     * <code>ZPath.from("a", "b").at("c")</code> represents the path "/a/b/c"
     *
     * @param child child node name
     * @return ZPath
     */
    ZPath at(String child);

    /**
     * Return this ZPath's parent
     *
     * @return parent ZPath
     * @throws java.util.NoSuchElementException if this is the root ZPath
     */
    ZPath parent();

    /**
     * Return true/false if this is the root ZPath
     *
     * @return true false
     */
    boolean isRoot();

    /**
     * The string full path that this ZPath represents
     *
     * @return full path
     */
    String fullPath();

    /**
     * The string parent path of this ZPath
     *
     * @return parent path
     * @throws java.util.NoSuchElementException if this is the root ZPath
     */
    String parentPath();

    /**
     * The node name at this ZPath
     *
     * @return name
     */
    String nodeName();

    /**
     * Return a regex Pattern useful for using in {@link org.apache.curator.framework.schema.Schema}
     *
     * @return pattern for this path
     */
    Pattern toSchemaPathPattern();
}
