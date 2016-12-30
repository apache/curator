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

public class SingleLevelCacheFilter implements CacheFilter
{
    private final String levelPath;
    private final CacheAction defaultAction;
    private final boolean isRoot;

    public SingleLevelCacheFilter(String levelPath)
    {
        this(levelPath, CacheAction.PATH_AND_DATA);
    }

    public SingleLevelCacheFilter(String levelPath, CacheAction defaultAction)
    {
        this.levelPath = levelPath;
        this.defaultAction = defaultAction;
        isRoot = levelPath.equals(ZKPaths.PATH_SEPARATOR);
    }

    @Override
    public CacheAction actionForPath(String path)
    {
        if ( isRoot && path.equals(ZKPaths.PATH_SEPARATOR) )    // special case. The parent of "/" is "/"
        {
            return CacheAction.NOT_STORED;
        }
        else if ( ZKPaths.getPathAndNode(path).getPath().equals(levelPath) )
        {
            return actionForMatchedPath();
        }
        return CacheAction.NOT_STORED;
    }

    protected CacheAction actionForMatchedPath()
    {
        return defaultAction;
    }
}
