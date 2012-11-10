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

package com.netflix.curator.framework.recipes.cache;

class ForceRefreshOperation implements Operation
{
    private final PathChildrenCache cache;

    ForceRefreshOperation(PathChildrenCache cache)
    {
        this.cache = cache;
    }

    @Override
    public void invoke() throws Exception
    {
        cache.refresh(true);
    }

    @Override
    public int hashCode()
    {
        return ForceRefreshOperation.class.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        //noinspection SimplifiableIfStatement
        if ( obj == null )
        {
            return false;
        }

        return (this == obj) || (getClass().equals(obj.getClass()));
    }

    @Override
    public String toString()
    {
        return "ForceRefreshOperation{}";
    }
}
