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

/**
 * <p>
 *     Controls which nodes a CuratorCache processes. When iterating
 *     over the children of a parent node, a given node's children are
 *     queried only if {@link #traverseChildren(String, String)} returns true.
 *     When caching the list of nodes for a parent node, a given node is
 *     stored only if {@link #actionForPath(String, String)} returns other than
 *     {@link CacheAction#NOT_STORED}.
 * </p>
 *
 * <p>
 *     E.g. Given:
 * <pre>
 * root
 *     n1-a
 *     n1-b
 *         n2-a
 *         n2-b
 *             n3-a
 *     n1-c
 *     n1-d
 * </pre>
 *     You could have a CuratorCache only work with the nodes: n1-a, n1-b, n2-a, n2-b, n1-d
 *     by returning false from traverseChildren() for "/root/n1-b/n2-b" and returning
 *     {@link CacheAction#NOT_STORED} from acceptChild("/root/n1-c").
 * </p>
 */
public interface CacheSelector
{
    /**
     * Return true if children of this path should be cached.
     * i.e. if false is returned, this node is not queried to
     * determine if it has children or not
     *
     * @param basePath the main path of the cache
     * @param fullPath full path of the ZNode
     * @return true/false
     */
    boolean traverseChildren(String basePath, String fullPath);

    /**
     * Return true if this node should be returned from the cache
     *
     * @param basePath the main path of the cache
     * @param fullPath full path of the ZNode
     * @return true/false
     */
    CacheAction actionForPath(String basePath, String fullPath);
}
