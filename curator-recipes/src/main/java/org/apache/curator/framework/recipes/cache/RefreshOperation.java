/*
 * Copyright 2012 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.apache.curator.framework.recipes.cache;

class RefreshOperation implements Operation
{
    private final PathChildrenCache cache;
    private final PathChildrenCache.RefreshMode mode;

    RefreshOperation(PathChildrenCache cache, PathChildrenCache.RefreshMode mode)
    {
        this.cache = cache;
        this.mode = mode;
    }

    @Override
    public void invoke() throws Exception
    {
        cache.refresh(mode);
    }

    @Override
    public boolean equals(Object o)
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        RefreshOperation that = (RefreshOperation)o;

        //noinspection RedundantIfStatement
        if ( mode != that.mode )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return mode.hashCode();
    }

    @Override
    public String toString()
    {
        return "RefreshOperation(" + mode + "){}";
    }
}
