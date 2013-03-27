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
package org.apache.curator.x.discovery;

import org.apache.curator.x.discovery.strategies.RoundRobinStrategy;
import java.io.Closeable;
import java.util.Collection;

public interface ServiceDiscovery<T> extends Closeable
{
    /**
     * The discovery must be started before use
     *
     * @throws Exception errors
     */
    public void start() throws Exception;

    /**
     * Register/re-register a service
     *
     * @param service service to add
     * @throws Exception errors
     */
    public void     registerService(ServiceInstance<T> service) throws Exception;

    /**
     * Update a service
     *
     * @param service service to update
     * @throws Exception errors
     */
    public void     updateService(ServiceInstance<T> service) throws Exception;

    /**
     * Unregister/remove a service instance
     *
     * @param service the service
     * @throws Exception errors
     */
    public void     unregisterService(ServiceInstance<T> service) throws Exception;

    /**
     * Allocate a new service cache builder. The refresh padding is defaulted to 1 second.
     *
     * @return new cache builder
     */
    public ServiceCacheBuilder<T> serviceCacheBuilder();

    /**
     * Return the names of all known services
     *
     * @return list of service names
     * @throws Exception errors
     */
    public Collection<String> queryForNames() throws Exception;

    /**
     * Return all known instances for the given service
     *
     * @param name name of the service
     * @return list of instances (or an empty list)
     * @throws Exception errors
     */
    public Collection<ServiceInstance<T>>  queryForInstances(String name) throws Exception;

    /**
     * Return a service instance POJO
     *
     * @param name name of the service
     * @param id ID of the instance
     * @return the instance or <code>null</code> if not found
     * @throws Exception errors
     */
    public ServiceInstance<T> queryForInstance(String name, String id) throws Exception;

    /**
     * Allocate a new builder. {@link ServiceProviderBuilder#providerStrategy} is set to {@link RoundRobinStrategy}
     * and {@link ServiceProviderBuilder#refreshPaddingMs} is set to 1 second.
     *
     * @return the builder
     */
    public ServiceProviderBuilder<T> serviceProviderBuilder();
}
