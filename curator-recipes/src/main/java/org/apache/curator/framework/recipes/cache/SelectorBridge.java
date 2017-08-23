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
package org.apache.curator.framework.recipes.cache;

import org.apache.curator.framework.recipes.watch.CacheAction;
import org.apache.curator.framework.recipes.watch.CacheSelector;
import java.util.Objects;

/**
 * Utility to bridge an old TreeCacheSelector to a new CuratorCache selector
 */
public class SelectorBridge implements CacheSelector
{
    private final TreeCacheSelector selector;
    private final CacheAction action;

    /**
     * Builder style constructor
     *
     * @param selector the old TreeCacheSelector to bridge
     * @return bridged selector
     */
    public static SelectorBridge wrap(TreeCacheSelector selector)
    {
        return new SelectorBridge(selector);
    }

    /**
     * @param selector the old TreeCacheSelector to bridge
     */
    public SelectorBridge(TreeCacheSelector selector)
    {
        this(selector, CacheAction.STAT_AND_DATA);
    }

    /**
     * @param selector the old TreeCacheSelector to bridge
     * @param action value to return for active paths
     */
    public SelectorBridge(TreeCacheSelector selector, CacheAction action)
    {
        this.selector = Objects.requireNonNull(selector, "selector cannot be null");
        this.action = Objects.requireNonNull(action, "action cannot be null");
    }

    @Override
    public boolean traverseChildren(String basePath, String fullPath)
    {
        return selector.traverseChildren(fullPath);
    }

    @Override
    public CacheAction actionForPath(String basePath, String fullPath)
    {
        return selector.acceptChild(fullPath) ? action : CacheAction.NOT_STORED;
    }
}
