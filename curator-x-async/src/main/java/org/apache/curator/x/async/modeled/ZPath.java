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
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;

import static org.apache.curator.utils.ZKPaths.PATH_SEPARATOR;

/**
 * Abstracts a ZooKeeper ZNode path
 */
public interface ZPath extends Resolvable
{
    /**
     * The root path: "/"
     */
    ZPath root = ZPathImpl.root;

    /**
     * Returns the special node name that can be used for replacements at runtime
     * via {@link #resolved(Object...)} when passed via the various <code>from()</code> methods
     */
    static String parameter()
    {
        return parameter("id");
    }

    /**
     * Same as {@link #parameter()} but allows you to specify an alternate code/name. This name
     * has no effect and is only for debugging purposes. When <code>toString()</code> is called
     * on ZPaths, this code shows.
     */
    static String parameter(String name)
    {
        return PATH_SEPARATOR + "{" + name + "}";
    }

    /**
     * Take a string path and return a ZPath
     *
     * @param fullPath the path to parse
     * @return ZPath
     * @throws IllegalArgumentException if the path is invalid
     */
    static ZPath parse(String fullPath)
    {
        return ZPathImpl.parse(fullPath, s -> s);
    }

    /**
     * Take a string path and return a ZPath. Each part of the path
     * that is <code>{XXXX}</code> is replaced with {@link #parameter()}.
     * E.g. <code>parseWithIds("/one/two/{first}/three/{second}")</code> is the equivalent
     * of calling <code>ZPath.from("one", "two", parameter(), "three", parameter())</code>
     *
     * @param fullPath the path to parse
     * @return ZPath
     * @throws IllegalArgumentException if the path is invalid
     */
    static ZPath parseWithIds(String fullPath)
    {
        return ZPathImpl.parse(fullPath, s -> isId(s) ? (PATH_SEPARATOR + s) : s); // TODO
    }

    /**
     * Return true if the given string conforms to the "{XXXX}" ID pattern
     *
     * @param s string to check
     * @return true/false
     */
    static boolean isId(String s)
    {
        return s.startsWith("{") && s.endsWith("}");
    }

    /**
     * Take a ZNode string path and return a ZPath
     *
     * @param fullPath the path to parse
     * @param nameFilter each part of the path is passed through this filter
     * @return ZPath
     * @throws IllegalArgumentException if the path is invalid
     */
    static ZPath parse(String fullPath, UnaryOperator<String> nameFilter)
    {
        return ZPathImpl.parse(fullPath, nameFilter);
    }

    /**
     * Convert individual path names into a ZPath. E.g.
     * <code>ZPath.from("my", "full", "path")</code>. Any/all of the names can be passed as
     * {@link #parameter()} so that the path can be resolved later using
     * of the <code>resolved()</code> methods.
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
     * Convert individual path names into a ZPath. Any/all of the names can be passed as
     * {@link #parameter()} so that the path can be resolved later using
     * of the <code>resolved()</code> methods.
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
     * Convert individual path names into a ZPath starting at the given base. E.g.
     * if base is "/home/base" <code>ZPath.from(base, "my", "full", "path")</code>
     * would be "/home/base/my/full/path". Any/all of the names can be passed as
     * {@link #parameter()} so that the path can be resolved later using
     * of the <code>resolved()</code> methods.
     *
     * @param base base/starting path
     * @param names path names
     * @return ZPath
     * @throws IllegalArgumentException if any of the names is invalid
     */
    static ZPath from(ZPath base, String... names)
    {
        return ZPathImpl.from(base, names);
    }

    /**
     * Convert individual path names into a ZPath starting at the given base. Any/all of the names can be passed as
     * {@link #parameter()} so that the path can be resolved later using
     * of the <code>resolved()</code> methods.
     *
     * @param base base/starting path
     * @param names path names
     * @return ZPath
     * @throws IllegalArgumentException if any of the names is invalid
     */
    static ZPath from(ZPath base, List<String> names)
    {
        return ZPathImpl.from(base, names);
    }

    /**
     * <p>
     *     When creating paths, any node in the path can be set to {@link #parameter()}.
     *     At runtime, the ZPath can be "resolved" by replacing these nodes with values.
     * </p>
     *
     * <p>
     *     The replacement is the <code>toString()</code> value of the parameter object or,
     *     if the object implements {@link org.apache.curator.x.async.modeled.NodeName},
     *     the value of <code>nodeName()</code>.
     * </p>
     *
     * @param parameters list of replacements. Must have be the same length as the number of
     *                   parameter nodes in the path
     * @return new resolved ZPath
     */
    @Override
    default ZPath resolved(Object... parameters)
    {
        return resolved(Arrays.asList(parameters));
    }

    /**
     * <p>
     *     When creating paths, any node in the path can be set to {@link #parameter()}.
     *     At runtime, the ZPath can be "resolved" by replacing these nodes with values.
     * </p>
     *
     * <p>
     *     The replacement is the <code>toString()</code> value of the parameter object or,
     *     if the object implements {@link org.apache.curator.x.async.modeled.NodeName},
     *     the value of <code>nodeName()</code>.
     * </p>
     *
     * @param parameters list of replacements. Must have be the same length as the number of
     *                   parameter nodes in the path
     * @return new resolved ZPath
     */
    @Override
    ZPath resolved(List<Object> parameters);

    /**
     * <p>
     *     Return a ZPath that represents a child ZNode of this ZPath. e.g.
     *     <code>ZPath.from("a", "b").at("c")</code> represents the path "/a/b/c"
     * </p>
     *
     * <p>
     *     The replacement is the <code>toString()</code> value of child or,
     *     if it implements {@link org.apache.curator.x.async.modeled.NodeName},
     *     the value of <code>nodeName()</code>.
     * </p>
     *
     * @param child child node name
     * @return ZPath
     */
    ZPath at(Object child);

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
     * Return true if this path starts with the given path. i.e.
     * <code>ZPath.from("/one/two/three").startsWith(ZPath.from("/one/two"))</code> returns true
     *
     * @param path base path
     * @return true/false
     */
    boolean startsWith(ZPath path);

    /**
     * The string full path that this ZPath represents
     *
     * @return full path
     */
    String fullPath();

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
