/*
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

package org.apache.curator.x.discovery;

import java.util.concurrent.ExecutorService;
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
     * Optional thread factory to use for the cache's internal thread. The specified ExecutorService
     * overrides any prior ThreadFactory or ExecutorService set on the ServiceCacheBuilder.
     *
     * @param threadFactory factory
     * @return this
     * @deprecated use {@link #executorService(ExecutorService)} instead
     */
    @Deprecated
    public ServiceCacheBuilder<T> threadFactory(ThreadFactory threadFactory);

    /**
     * Optional ExecutorService to use for the cache's background thread. The specified ExecutorService
     * will be wrapped in a CloseableExecutorService and overrides any prior ThreadFactory or ExecutorService
     * set on the ServiceCacheBuilder.
     *
     * @param executorService executor service
     * @return this
     */
    public ServiceCacheBuilder<T> executorService(ExecutorService executorService);
}
