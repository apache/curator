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

package org.apache.curator.x.rest.system;

import com.google.common.io.Closeables;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import java.util.concurrent.Future;

public interface ThingType<T>
{
    public static ThingType<InterProcessSemaphoreMutex> MUTEX = new ThingType<InterProcessSemaphoreMutex>()
    {
        @Override
        public Class<InterProcessSemaphoreMutex> getThingClass()
        {
            return InterProcessSemaphoreMutex.class;
        }

        @Override
        public void closeFor(InterProcessSemaphoreMutex instance)
        {
            // nop
        }
    };

    public static ThingType<PathChildrenCache> PATH_CACHE = new ThingType<PathChildrenCache>()
    {
        @Override
        public Class<PathChildrenCache> getThingClass()
        {
            return PathChildrenCache.class;
        }

        @Override
        public void closeFor(PathChildrenCache cache)
        {
            Closeables.closeQuietly(cache);
        }
    };

    public static ThingType<Future> FUTURE = new ThingType<Future>()
    {
        @Override
        public Class<Future> getThingClass()
        {
            return Future.class;
        }

        @Override
        public void closeFor(Future future)
        {
            future.cancel(true);
        }
    };

    public Class<T> getThingClass();

    public void closeFor(T instance);
}
