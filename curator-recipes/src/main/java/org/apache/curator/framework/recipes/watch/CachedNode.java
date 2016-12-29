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

import org.apache.zookeeper.data.Stat;
import java.util.Arrays;
import java.util.Objects;

public class CachedNode
{
    private final Stat stat;
    private final byte[] data;

    private static final byte[] defaultData = new byte[0];

    public CachedNode()
    {
        this(new Stat(), defaultData);
    }

    public CachedNode(Stat stat)
    {
        this(stat, defaultData);
    }

    public CachedNode(Stat stat, byte[] data)
    {
        this.stat = Objects.requireNonNull(stat, "stat cannot be null");
        this.data = Objects.requireNonNull(data, "data cannot be null");
    }

    public Stat getStat()
    {
        return stat;
    }

    public byte[] getData()
    {
        return data;
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

        CachedNode that = (CachedNode)o;

        //noinspection SimplifiableIfStatement
        if ( !stat.equals(that.stat) )
        {
            return false;
        }
        return Arrays.equals(data, that.data);
    }

    @Override
    public int hashCode()
    {
        int result = stat.hashCode();
        result = 31 * result + Arrays.hashCode(data);
        return result;
    }

    @Override
    public String toString()
    {
        return "CachedNode{" + "stat=" + stat + ", data=" + Arrays.toString(data) + '}';
    }
}
