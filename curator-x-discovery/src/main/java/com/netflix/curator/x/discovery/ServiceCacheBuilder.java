/*
 *
 *  Copyright 2011 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package com.netflix.curator.x.discovery;

import java.util.concurrent.ThreadFactory;

public interface ServiceCacheBuilder<T>
{
    /**
     * Return a new service cache with the current settings
     *
     * @return service cache
     */
    public ServiceCache<T> build();

    /**
     * The name of the service to cache (required)
     *
     * @param name service name
     * @return this
     */
    public ServiceCacheBuilder<T> name(String name);

    /**
     * Optional thread factory to use for the cache's internal thread
     *
     * @param threadFactory factory
     * @return this
     */
    public ServiceCacheBuilder<T> threadFactory(ThreadFactory threadFactory);
}
