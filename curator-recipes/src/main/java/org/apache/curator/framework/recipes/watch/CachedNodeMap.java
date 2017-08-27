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
package org.apache.curator.framework.recipes.watch;

import java.util.Collection;
import java.util.Map;

/**
 * Interface for a container of path to {@link CachedNodeImpl}
 */
public interface CachedNodeMap
{
    /**
     * Return true if the container is empty
     *
     * @return true/false
     */
    boolean isEmpty();

    /**
     * Return the number of nodes in the container
     *
     * @return size
     */
    int size();

    /**
     * Remove the mode mapped to the given path and return it. If there is no node
     * mapped to the path, return <code>null</code>
     *
     * @param path path
     * @return removed node or null
     */
    CachedNode remove(String path);

    /**
     * Remove any/all nodes from the container
     */
    void invalidateAll();

    /**
     * Return true if there currently is a node mapped to the given path
     *
     * @param path path
     * @return true/false
     */
    boolean containsKey(String path);

    /**
     * Return an unmodifiable set of paths currently in the container
     *
     * @return path set
     */
    Collection<? extends String> pathSet();

    /**
     * Return an unmodifiable map-style view of the container
     *
     * @return map
     */
    Map<String, CachedNode> view();

    /**
     * Return an unmodifiable map-style entry set of the container
     *
     * @return entry set
     */
    Iterable<? extends Map.Entry<String, CachedNode>> entrySet();

    /**
     * Return the node mapped to the given path or <code>null</code>
     *
     * @param path path
     * @return node or null
     */
    CachedNode get(String path);

    /**
     * Replaces the entry for the specified path only if currently mapped node is the the specified value.
     *
     * @param path path
     * @param oldNode old mapped node
     * @param newNode new node
     * @return true if the path was mapped to the oldNode
     */
    boolean replace(String path, CachedNode oldNode, CachedNode newNode);

    /**
     * Map the given node to the given path. Return the old mapping or <code>null</code>
     *
     * @param path path
     * @param node node
     * @return old value or null
     */
    CachedNode put(String path, CachedNode node);

    /**
     * Clean any internal data structures, etc. This method is called when the CachedNodeMap
     * will no longer be used
     */
    void cleanUp();
}
