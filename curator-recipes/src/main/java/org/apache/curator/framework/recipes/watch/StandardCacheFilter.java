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

class StandardCacheFilter implements CacheFilter
{
    private final CacheAction cacheAction;

    StandardCacheFilter(CacheAction cacheAction)
    {
        this.cacheAction = cacheAction;
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
