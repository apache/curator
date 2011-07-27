/*
 Copyright 2011 Netflix, Inc.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
 */
package com.netflix.curator.framework.recipes.cache;

import org.apache.zookeeper.data.Stat;

@SuppressWarnings({"LoopStatementThatDoesntLoop"})
public class ChildData implements Comparable<ChildData>
{
    private final String    path;
    private final Stat      stat;
    private final byte[]    data;
    private final long      thisObjectCreationTimeMs = System.currentTimeMillis();

    ChildData(String path, Stat stat, byte[] data)
    {
        this.path = path;
        this.stat = stat;
        this.data = data;
    }

    boolean     isComplete(PathChildrenCacheMode mode)
    {
        boolean     isComplete = false;
        if ( path != null )
        {
            switch ( mode )
            {
                case CACHE_DATA_AND_STAT:
                {
                    isComplete = (stat != null) && (data != null);
                    break;
                }
                
                case CACHE_DATA:
                {
                    isComplete = (data != null);
                    break;
                }

                case CACHE_PATHS_ONLY:
                {
                    isComplete = true;
                    break;
                }
            }
        }

        return isComplete;
    }

    long        getThisObjectCreationTimeMs()
    {
        return thisObjectCreationTimeMs;
    }

    @Override
    public int compareTo(ChildData rhs)
    {
        if ( this == rhs )
        {
            return 0;
        }
        if ( rhs == null || getClass() != rhs.getClass() )
        {
            return -1;
        }

        return path.compareTo(rhs.path);
    }

    @SuppressWarnings({"RedundantIfStatement"})
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

        ChildData childData = (ChildData)o;
        return compareTo(childData) == 0;
    }

    @Override
    public int hashCode()
    {
        return path.hashCode();
    }

    /**
     * Returns the full path of the this child
     *
     * @return full path
     */
    public String getPath()
    {
        return path;
    }

    /**
     * Returns the stat data for this child when the cache mode is {@link PathChildrenCacheMode#CACHE_DATA_AND_STAT}
     *
     * @return stat or null
     */
    public Stat getStat()
    {
        return stat;
    }

    /**
     * <p>Returns the node data for this child when the cache mode is {@link PathChildrenCacheMode#CACHE_DATA_AND_STAT}
     * or {@link PathChildrenCacheMode#CACHE_DATA}.</p>
     *
     * <p><b>NOTE:</b> the byte array returned is the raw reference of this instance's field. If you change
     * the values in the array any other callers to this method will see the change.</p>
     *
     * @return node data or null
     */
    public byte[] getData()
    {
        return data;
    }

    ChildData setStat(Stat stat)
    {
        return new ChildData(path, stat, data);
    }

    ChildData setData(byte[] data)
    {
        return new ChildData(path, stat, data);
    }
}
