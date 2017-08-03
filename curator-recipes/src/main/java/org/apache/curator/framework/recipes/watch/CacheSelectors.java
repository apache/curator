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

import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.server.PathIterator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Pre-built caching selectors
 */
public class CacheSelectors
{
    private static final CacheSelector statAndData = new StandardCacheSelector(CacheAction.STAT_AND_DATA);
    private static final CacheSelector uncompressedStatAndData = new StandardCacheSelector(CacheAction.STAT_AND_UNCOMPRESSED_DATA);
    private static final CacheSelector uncompressedStatOnly = new StandardCacheSelector(CacheAction.UNCOMPRESSED_STAT_ONLY);
    private static final CacheSelector statOnly = new StandardCacheSelector(CacheAction.STAT_ONLY);
    private static final CacheSelector pathOnly = new StandardCacheSelector(CacheAction.PATH_ONLY);

    private static class CompositeEntry
    {
        final String path;
        final CacheSelector selector;
        final int length;

        private CompositeEntry(String path, CacheSelector selector, int length)
        {
            this.path = path;
            this.selector = selector;
            this.length = length;
        }
    }

    private static class StandardCacheSelector implements CacheSelector
    {
        private final CacheAction cacheAction;

        private StandardCacheSelector(CacheAction cacheAction)
        {
            this.cacheAction = cacheAction;
        }

        @Override
        public boolean traverseChildren(String basePath, String fullPath)
        {
            return true;
        }

        @Override
        public CacheAction actionForPath(String basePath, String fullPath)
        {
            return cacheAction;
        }
    }

    private static class SingleLevelCacheSelector implements CacheSelector
    {
        private final CacheAction cacheAction;

        private SingleLevelCacheSelector(CacheAction cacheAction)
        {
            this.cacheAction = cacheAction;
        }

        @Override
        public boolean traverseChildren(String basePath, String fullPath)
        {
            return basePath.equals(fullPath);
        }

        @Override
        public CacheAction actionForPath(String mainPath, String checkPath)
        {
            boolean mainPathIsRoot = mainPath.endsWith(ZKPaths.PATH_SEPARATOR);
            if ( mainPathIsRoot && checkPath.equals(ZKPaths.PATH_SEPARATOR) )    // special case. The parent of "/" is "/"
            {
                return CacheAction.NOT_STORED;
            }
            else if ( ZKPaths.getPathAndNode(checkPath).getPath().equals(mainPath) )
            {
                return cacheAction;
            }
            return CacheAction.NOT_STORED;
        }
    }

    private static class MaxDepthCacheSelector implements CacheSelector
    {
        private final int maxDepth;
        private final CacheAction cacheAction;

        private MaxDepthCacheSelector(int maxDepth, CacheAction cacheAction)
        {
            this.maxDepth = maxDepth;
            this.cacheAction = cacheAction;
        }

        @Override
        public boolean traverseChildren(String basePath, String fullPath)
        {
            int mainPathDepth = ZKPaths.split(basePath).size();
            int checkPathDepth = ZKPaths.split(fullPath).size();
            int thisDepth = checkPathDepth - mainPathDepth;
            return (thisDepth <= maxDepth);
        }

        @Override
        public CacheAction actionForPath(String basePath, String fullPath)
        {
            return traverseChildren(basePath, fullPath) ? cacheAction : CacheAction.NOT_STORED;
        }
    }

    /**
     * Returns a cache selector that stores stat and data and processes the entire tree
     * from the root path given to the cache builder
     *
     * @return selector
     */
    public static CacheSelector statAndData()
    {
        return statAndData;
    }

    /**
     * Returns a cache selector that stores stat and uncompressed data and processes the entire tree
     * from the root path given to the cache builder
     *
     * @return selector
     */
    public static CacheSelector uncompressedStatAndData()
    {
        return uncompressedStatAndData;
    }

    /**
     * Returns a cache selector that stores only the stat and processes the entire tree
     * from the root path given to the cache builder. In events, however, uncompressed data is
     * sent (but not stored).
     *
     * @return selector
     */
    public static CacheSelector getUncompressedStatOnly()
    {
        return uncompressedStatOnly;
    }

    /**
     * Returns a cache selector that stores only the stat and processes the entire tree
     * from the root path given to the cache builder
     *
     * @return selector
     */
    public static CacheSelector statOnly()
    {
        return statOnly;
    }

    /**
     * Returns a cache selector that stores only the path and processes the entire tree
     * from the root path given to the cache builder
     *
     * @return selector
     */
    public static CacheSelector pathOnly()
    {
        return pathOnly;
    }

    /**
     * A selector that stores stat and data for the immediate child of the root path
     * given to the cache builder (exclusive of the root itself). This selector emulates
     * the deprecated {@link PathChildrenCache}
     *
     * @return selector
     */
    @SuppressWarnings("deprecation")
    public static CacheSelector singleLevel()
    {
        return new SingleLevelCacheSelector(CacheAction.STAT_AND_DATA);
    }

    /**
     * A selector that returns <code>cacheAction</code> for the immediate child of the root path
     * given to the cache builder (exclusive of the root itself). This selector emulates
     * the deprecated {@link PathChildrenCache}
     *
     * @param cacheAction the action to return
     * @return selector
     */
    @SuppressWarnings("deprecation")
    public static CacheSelector singleLevel(CacheAction cacheAction)
    {
        return new SingleLevelCacheSelector(cacheAction);
    }

    /**
     * A selector that stores stat and data at a maximum depth to explore/watch.
     * A {@code maxDepth} of {@code 0} will watch only
     * the root node; a {@code maxDepth} of {@code 1} will watch the
     * root node and its immediate children
     *
     * @param maxDepth depth to use
     * @return selector
     */
    public static CacheSelector maxDepth(int maxDepth)
    {
        return maxDepth(maxDepth, CacheAction.STAT_AND_DATA);
    }

    /**
     * A selector that returns <code>cacheAction</code> at a maximum depth to explore/watch.
     * A {@code maxDepth} of {@code 0} will watch only
     * the root node; a {@code maxDepth} of {@code 1} will watch the
     * root node and its immediate children
     *
     * @param cacheAction the action to return
     * @param maxDepth depth to use
     * @return selector
     */
    public static CacheSelector maxDepth(int maxDepth, CacheAction cacheAction)
    {
        return new MaxDepthCacheSelector(maxDepth, cacheAction);
    }

    /**
     * <p>
     *     Creates a composite cache selector that applies different selectors for different parent
     *     paths. The given selectorMap maps a parent path to a cache selector.
     * </p>
     *
     * <p>
     *     e.g. a map such as: <code>/a/b -> selectorA, /a/c -> selectorB</code> will apply "selectorA"
     *     to paths that are children of "/a/b", "a/b/x", "a/b/x/y", etc. and "selectorB" to paths that are children of
     *     "a/c", etc.
     * </p>
     *
     * <p>
     *     If no matches are found in the map, {@link #statAndData()} is used. NOTE: the longest
     *     paths (counted by segments) take precedence. i.e. the mapping of "/a/b/c" will match
     *     before "/a/b".
     * </p>
     *
     * @param selectorMap the selector map
     * @return the composite
     */
    public static CacheSelector composite(Map<String, CacheSelector> selectorMap)
    {
        return composite(selectorMap, statAndData);
    }

    /**
     * Same as {@link #composite(Map)} but uses the given default selector instead of {@link #statAndData()}.
     *
     * @param selectorMap the selector map
     * @param defaultSelector default selector if no match is found in the map
     * @return the composite
     */
    public static CacheSelector composite(Map<String, CacheSelector> selectorMap, final CacheSelector defaultSelector)
    {
        final List<CompositeEntry> entries = new ArrayList<>();
        for ( Map.Entry<String, CacheSelector> entry : selectorMap.entrySet() )
        {
            String path = entry.getKey();
            PathUtils.validatePath(path);
            CacheSelector selector = entry.getValue();
            int length = path.split("/").length;
            entries.add(new CompositeEntry(path, selector, length));
        }
        Collections.sort(entries, new Comparator<CompositeEntry>()
        {
            @Override
            public int compare(CompositeEntry e1, CompositeEntry e2)
            {
                return e2.length - e1.length;   // reverse length order
            }
        });

        return new CacheSelector()
        {
            @Override
            public boolean traverseChildren(String basePath, String fullPath)
            {
                return getSelector(fullPath).traverseChildren(basePath, fullPath);
            }

            @Override
            public CacheAction actionForPath(String basePath, String fullPath)
            {
                return getSelector(fullPath).actionForPath(basePath, fullPath);
            }

            private CacheSelector getSelector(String fullPath)
            {
                String parent = ZKPaths.getPathAndNode(fullPath).getPath();
                for ( CompositeEntry entry : entries )
                {
                    PathIterator pathIterator = new PathIterator(fullPath);
                    while ( pathIterator.hasNext() )
                    {
                        if ( pathIterator.next().equals(entry.path) )
                        {
                            return entry.selector;
                        }
                    }
                }
                return defaultSelector;
            }
        };
    }

    private CacheSelectors()
    {
    }
}
