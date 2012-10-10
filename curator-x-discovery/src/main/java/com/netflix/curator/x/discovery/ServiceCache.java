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

package com.netflix.curator.x.discovery;

import com.netflix.curator.framework.listen.Listenable;
import com.netflix.curator.x.discovery.details.InstanceProvider;
import com.netflix.curator.x.discovery.details.ServiceCacheListener;
import java.io.Closeable;
import java.util.List;

public interface ServiceCache<T> extends Closeable, Listenable<ServiceCacheListener>, InstanceProvider<T>
{
    /**
     * Return the current list of instances. NOTE: there is no guarantee of freshness. This is
     * merely the last known list of instances. However, the list is updated via a ZooKeeper watcher
     * so it should be fresh within a window of a second or two.
     *
     * @return the list
     */
    public List<ServiceInstance<T>> getInstances();

    /**
     * The cache must be started before use
     *
     * @throws Exception errors
     */
    public void start() throws Exception;
}
