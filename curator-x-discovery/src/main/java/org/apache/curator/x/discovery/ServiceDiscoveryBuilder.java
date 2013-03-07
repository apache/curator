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

package org.apache.curator.x.discovery;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.x.discovery.details.InstanceSerializer;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;
import org.apache.curator.x.discovery.details.ServiceDiscoveryImpl;

public class ServiceDiscoveryBuilder<T>
{
    private CuratorFramework        client;
    private String                  basePath;
    private InstanceSerializer<T>   serializer;
    private ServiceInstance<T>      thisInstance;

    /**
     * Return a new builder. The builder will be defaulted with a {@link JsonInstanceSerializer}.
     *
     * @param payloadClass the class of the payload of your service instance (you can use {@link Void}
     * if your instances don't need a payload)
     * @return new builder
     */
    public static<T> ServiceDiscoveryBuilder<T>     builder(Class<T> payloadClass)
    {
        return new ServiceDiscoveryBuilder<T>(payloadClass).serializer(new JsonInstanceSerializer<T>(payloadClass));
    }

    /**
     * Build a new service discovery with the currently set values
     *
     * @return new service discovery
     */
    public ServiceDiscovery<T>      build()
    {
        return new ServiceDiscoveryImpl<T>(client, basePath, serializer, thisInstance);
    }

    /**
     * Required - set the client to use
     *
     * @param client client
     * @return this
     */
    public ServiceDiscoveryBuilder<T>   client(CuratorFramework client)
    {
        this.client = client;
        return this;
    }

    /**
     * Required - set the base path to store in ZK
     *
     * @param basePath base path
     * @return this
     */
    public ServiceDiscoveryBuilder<T>   basePath(String basePath)
    {
        this.basePath = basePath;
        return this;
    }

    /**
     * optional - change the serializer used (the default is {@link JsonInstanceSerializer}
     *
     * @param serializer the serializer
     * @return this
     */
    public ServiceDiscoveryBuilder<T>   serializer(InstanceSerializer<T> serializer)
    {
        this.serializer = serializer;
        return this;
    }

    /**
     * Optional - instance that represents the service that is running. The instance will get auto-registered
     *
     * @param thisInstance initial instance
     * @return this
     */
    public ServiceDiscoveryBuilder<T>   thisInstance(ServiceInstance<T> thisInstance)
    {
        this.thisInstance = thisInstance;
        return this;
    }

    ServiceDiscoveryBuilder(Class<T> payloadClass)
    {
    }
}
