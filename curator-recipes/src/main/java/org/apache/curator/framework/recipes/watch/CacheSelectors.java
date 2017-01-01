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

import org.apache.curator.utils.ZKPaths;

public class CacheSelectors
{
    private static final CacheSelector statAndData = new StandardCacheSelector(CacheAction.STAT_AND_DATA);
    private static final CacheSelector compressedStatAndData = new StandardCacheSelector(CacheAction.STAT_AND_COMPRESSED_DATA);
    private static final CacheSelector statOnly = new StandardCacheSelector(CacheAction.STAT_ONLY);
    private static final CacheSelector pathOnly = new StandardCacheSelector(CacheAction.PATH_ONLY);

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

    public static CacheSelector statAndData()
    {
        return statAndData;
    }

    public static CacheSelector compressedStatAndData()
    {
        return compressedStatAndData;
    }

    public static CacheSelector statOnly()
    {
        return statOnly;
    }

    public static CacheSelector pathOnly()
    {
        return pathOnly;
    }

    public static CacheSelector singleLevel()
    {
        return new SingleLevelCacheSelector(CacheAction.STAT_AND_DATA);
    }

    public static CacheSelector singleLevel(CacheAction cacheAction)
    {
        return new SingleLevelCacheSelector(cacheAction);
    }

    public static CacheSelector maxDepth(int maxDepth)
    {
        return maxDepth(maxDepth, CacheAction.STAT_AND_DATA);
    }

    public static CacheSelector maxDepth(int maxDepth, CacheAction cacheAction)
    {
        return new MaxDepthCacheSelector(maxDepth, cacheAction);
    }

    private CacheSelectors()
    {
    }
}
