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
package org.apache.curator.x.async.modeled.recipes;

import org.apache.curator.x.async.modeled.ZPath;
import org.apache.zookeeper.data.Stat;
import java.util.Objects;
import java.util.Optional;

public class ModeledCachedNode<T>
{
    private final ZPath path;
    private final Stat stat;
    private final Optional<T> data;

    public ModeledCachedNode(ZPath path)
    {
        this(path, null, new Stat());
    }

    public ModeledCachedNode(ZPath path, T data)
    {
        this(path, data, new Stat());
    }

    public ModeledCachedNode(ZPath path, T data, Stat stat)
    {
        this.path = Objects.requireNonNull(path, "path cannot be null");
        this.data = Optional.ofNullable(data);
        this.stat = Objects.requireNonNull(stat, "stat cannot be null");
    }

    public ZPath getPath()
    {
        return path;
    }

    public Stat getStat()
    {
        return stat;
    }

    public Optional<T> getData()
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

        ModeledCachedNode<?> that = (ModeledCachedNode<?>)o;

        if ( !path.equals(that.path) )
        {
            return false;
        }
        //noinspection SimplifiableIfStatement
        if ( !stat.equals(that.stat) )
        {
            return false;
        }
        return data.equals(that.data);
    }

    @Override
    public int hashCode()
    {
        int result = path.hashCode();
        result = 31 * result + stat.hashCode();
        result = 31 * result + data.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return "ModeledCachedNode{" + "stat=" + stat + ", data=" + data + '}';
    }
}
