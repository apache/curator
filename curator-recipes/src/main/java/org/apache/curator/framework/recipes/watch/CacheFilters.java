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

import java.util.Objects;

public class CacheFilters
{
    private static final CacheFilter statAndData = new StandardCacheFilter(CacheAction.STAT_AND_DATA);
    private static final CacheFilter compressedStatAndData = new StandardCacheFilter(CacheAction.STAT_AND_COMPRESSED_DATA);
    private static final CacheFilter statOnly = new StandardCacheFilter(CacheAction.STAT_ONLY);
    private static final CacheFilter pathOnly = new StandardCacheFilter(CacheAction.PATH_ONLY);
    private static final CacheFilter fullStatAndData = new CacheFilter()
    {
        @Override
        public CacheAction actionForPath(String mainPath, String checkPath)
        {
            return CacheAction.STAT_AND_DATA;
        }
    };
    private static final CacheFilter fullCompressedStatAndData = new CacheFilter()
    {
        @Override
        public CacheAction actionForPath(String mainPath, String checkPath)
        {
            return CacheAction.STAT_AND_COMPRESSED_DATA;
        }
    };
    private static final CacheFilter fullStatOnly = new CacheFilter()
    {
        @Override
        public CacheAction actionForPath(String mainPath, String checkPath)
        {
            return CacheAction.STAT_ONLY;
        }
    };
    private static final CacheFilter fullPathOnly = new CacheFilter()
    {
        @Override
        public CacheAction actionForPath(String mainPath, String checkPath)
        {
            return CacheAction.PATH_ONLY;
        }
    };

    public static CacheFilter statAndData()
    {
        return statAndData;
    }

    public static CacheFilter compressedData()
    {
        return compressedStatAndData;
    }

    public static CacheFilter statOnly()
    {
        return statOnly;
    }

    public static CacheFilter pathOnly()
    {
        return pathOnly;
    }

    public static CacheFilter fullStatAndData()
    {
        return fullStatAndData;
    }

    public static CacheFilter fullCompressedStatAndData()
    {
        return fullCompressedStatAndData;
    }

    public static CacheFilter fullStatOnly()
    {
        return fullStatOnly;
    }

    public static CacheFilter fullPathOnly()
    {
        return fullPathOnly;
    }

    static CacheFilter maxDepth(int maxDepth, final CacheFilter mainFilter)
    {
        final RefreshFilter refreshFilter = RefreshFilters.maxDepth(maxDepth);
        return new CacheFilter()
        {
            @Override
            public CacheAction actionForPath(String mainPath, String checkPath)
            {
                return refreshFilter.descend(mainPath, checkPath) ? mainFilter.actionForPath(mainPath, checkPath) : CacheAction.NOT_STORED;
            }
        };
    }

    private CacheFilters()
    {
    }
}
