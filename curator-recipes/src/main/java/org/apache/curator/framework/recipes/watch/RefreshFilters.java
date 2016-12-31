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

import org.apache.zookeeper.server.PathIterator;

public class RefreshFilters
{
    private static final RefreshFilter singleLevel = new RefreshFilter()
    {
        @Override
        public boolean descend(String mainPath, String checkPath)
        {
            return mainPath.equals(checkPath);
        }
    };

    private static final RefreshFilter tree = new RefreshFilter()
    {
        @Override
        public boolean descend(String mainPath, String checkPath)
        {
            return true;
        }
    };

    public static RefreshFilter singleLevel()
    {
        return singleLevel;
    }

    public static RefreshFilter tree()
    {
        return tree;
    }

    public static RefreshFilter maxDepth(final int maxDepth)
    {
        return new RefreshFilter()
        {
            @Override
            public boolean descend(String mainPath, String checkPath)
            {
                PathIterator pathIterator = new PathIterator(checkPath);
                int thisDepth = 1;
                while ( pathIterator.hasNext() )
                {
                    String thisParent = pathIterator.next();
                    if ( thisParent.equals(mainPath) )
                    {
                        break;
                    }
                    ++thisDepth;
                }
                return (thisDepth <= maxDepth);
            }
        };
    }

    private RefreshFilters()
    {
    }
}
