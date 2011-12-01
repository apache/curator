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

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.x.discovery.details.InstanceSerializer;
import com.netflix.curator.x.discovery.details.JsonInstanceSerializer;
import com.netflix.curator.x.discovery.details.ServiceDiscoveryImpl;

public class ServiceDiscoveryBuilder<T>
{
    private CuratorFramework        client;
    private String                  basePath;
    private InstanceSerializer<T>   serializer;
    private ServiceInstance<T>      thisInstance;

    public static<T> ServiceDiscoveryBuilder<T>     builder(Class<T> payloadClass)
    {
        return new ServiceDiscoveryBuilder<T>(payloadClass);
    }

    public ServiceDiscovery<T>      build()
    {
        return new ServiceDiscoveryImpl<T>(client, basePath, serializer, thisInstance);
    }

    public ServiceDiscoveryBuilder<T>   client(CuratorFramework client)
    {
        this.client = client;
        return this;
    }

    public ServiceDiscoveryBuilder<T>   basePath(String basePath)
    {
        this.basePath = basePath;
        return this;
    }

    public ServiceDiscoveryBuilder<T>   serializer(InstanceSerializer<T> serializer)
    {
        this.serializer = serializer;
        return this;
    }

    public ServiceDiscoveryBuilder<T>   thisInstance(ServiceInstance<T> thisInstance)
    {
        this.thisInstance = thisInstance;
        return this;
    }

    ServiceDiscoveryBuilder(Class<T> payloadClass)
    {
        serializer = new JsonInstanceSerializer<T>(payloadClass);
    }
}
