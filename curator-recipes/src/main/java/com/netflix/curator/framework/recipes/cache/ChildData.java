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

    boolean     isComplete()
    {
        return (path != null) && (stat != null) && (data != null);
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

    public ChildData setPath(String path)
    {
        return new ChildData(path, stat, data);
    }

    public ChildData setStat(Stat stat)
    {
        return new ChildData(path, stat, data);
    }

    public ChildData setData(byte[] data)
    {
        return new ChildData(path, stat, data);
    }

    public String getPath()
    {
        return path;
    }

    public Stat getStat()
    {
        return stat;
    }

    public byte[] getData()
    {
        return data;
    }
}
