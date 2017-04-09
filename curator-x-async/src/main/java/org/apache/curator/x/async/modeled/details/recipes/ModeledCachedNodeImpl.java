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
package org.apache.curator.x.async.modeled.details.recipes;

import org.apache.curator.x.async.modeled.ZPath;
import org.apache.curator.x.async.modeled.recipes.ModeledCachedNode;
import org.apache.zookeeper.data.Stat;
import java.util.Objects;

public class ModeledCachedNodeImpl<T> implements ModeledCachedNode<T>
{
    private final ZPath path;
    private final Stat stat;
    private final T data;

    public ModeledCachedNodeImpl(ZPath path)
    {
        this(path, null, new Stat());
    }

    public ModeledCachedNodeImpl(ZPath path, T data)
    {
        this(path, data, new Stat());
    }

    public ModeledCachedNodeImpl(ZPath path, T data, Stat stat)
    {
        this.path = Objects.requireNonNull(path, "path cannot be null");
        this.stat = Objects.requireNonNull(stat, "stat cannot be null");
        this.data = data;
    }

    @Override
    public ZPath getPath()
    {
        return path;
    }

    @Override
    public Stat getStat()
    {
        return stat;
    }

    @Override
    public T getModel()
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

        ModeledCachedNodeImpl<?> that = (ModeledCachedNodeImpl<?>)o;

        if ( !path.equals(that.path) )
        {
            return false;
        }
        //noinspection SimplifiableIfStatement
        if ( !stat.equals(that.stat) )
        {
            return false;
        }
        return data != null ? data.equals(that.data) : that.data == null;
    }

    @Override
    public int hashCode()
    {
        int result = path.hashCode();
        result = 31 * result + stat.hashCode();
        result = 31 * result + (data != null ? data.hashCode() : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return "ModeledCachedNode{" + "stat=" + stat + ", data=" + data + '}';
    }
}
