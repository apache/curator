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
package org.apache.curator.x.async.modeled.details;

import org.apache.curator.x.async.modeled.cached.ZNode;
import org.apache.curator.x.async.modeled.ZPath;
import org.apache.zookeeper.data.Stat;
import java.util.Objects;

public class ZNodeImpl<T> implements ZNode<T>
{
    private final ZPath path;
    private final Stat stat;
    private final T model;

    public ZNodeImpl(ZPath path, Stat stat, T model)
    {
        this.path = Objects.requireNonNull(path, "path cannot be null");
        this.stat = Objects.requireNonNull(stat, "stat cannot be null");
        this.model = Objects.requireNonNull(model, "model cannot be null");
    }

    @Override
    public ZPath path()
    {
        return path;
    }

    @Override
    public Stat stat()
    {
        return stat;
    }

    @Override
    public T model()
    {
        return model;
    }
}
