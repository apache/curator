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
package org.apache.curator.x.rpc.idl.discovery;

import com.facebook.swift.service.ThriftMethod;
import com.facebook.swift.service.ThriftService;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import org.apache.curator.utils.ThreadUtils;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.rpc.connections.ConnectionManager;
import org.apache.curator.x.rpc.connections.CuratorEntry;
import org.apache.curator.x.rpc.idl.exceptions.RpcException;
import org.apache.curator.x.rpc.idl.structs.CuratorProjection;
import java.util.Collection;

@ThriftService
public class DiscoveryServiceLowLevel
{
    private final ConnectionManager connectionManager;

    public DiscoveryServiceLowLevel(ConnectionManager connectionManager)
    {
        this.connectionManager = connectionManager;
    }

    @ThriftMethod
    public void registerInstance(CuratorProjection projection, DiscoveryProjection discoveryProjection, DiscoveryInstance instance) throws RpcException
    {
        try
        {
            CuratorEntry entry = CuratorEntry.mustGetEntry(connectionManager, projection);
            @SuppressWarnings("unchecked")
            ServiceDiscovery<byte[]> serviceDiscovery = CuratorEntry.mustGetThing(entry, discoveryProjection.id, ServiceDiscovery.class);
            serviceDiscovery.registerService(instance.toReal());
        }
        catch ( Exception e )
        {
            ThreadUtils.checkInterrupted(e);
            throw new RpcException(e);
        }
    }

    @ThriftMethod
    public void updateInstance(CuratorProjection projection, DiscoveryProjection discoveryProjection, DiscoveryInstance instance) throws RpcException
    {
        try
        {
            CuratorEntry entry = CuratorEntry.mustGetEntry(connectionManager, projection);
            @SuppressWarnings("unchecked")
            ServiceDiscovery<byte[]> serviceDiscovery = CuratorEntry.mustGetThing(entry, discoveryProjection.id, ServiceDiscovery.class);
            serviceDiscovery.updateService(instance.toReal());
        }
        catch ( Exception e )
        {
            ThreadUtils.checkInterrupted(e);
            throw new RpcException(e);
        }
    }

    @ThriftMethod
    public void unregisterInstance(CuratorProjection projection, DiscoveryProjection discoveryProjection, DiscoveryInstance instance) throws RpcException
    {
        try
        {
            CuratorEntry entry = CuratorEntry.mustGetEntry(connectionManager, projection);
            @SuppressWarnings("unchecked")
            ServiceDiscovery<byte[]> serviceDiscovery = CuratorEntry.mustGetThing(entry, discoveryProjection.id, ServiceDiscovery.class);
            serviceDiscovery.unregisterService(instance.toReal());
        }
        catch ( Exception e )
        {
            ThreadUtils.checkInterrupted(e);
            throw new RpcException(e);
        }
    }

    @ThriftMethod
    public Collection<String> queryForNames(CuratorProjection projection, DiscoveryProjection discoveryProjection) throws RpcException
    {
        CuratorEntry entry = CuratorEntry.mustGetEntry(connectionManager, projection);
        @SuppressWarnings("unchecked")
        ServiceDiscovery<byte[]> serviceDiscovery = CuratorEntry.mustGetThing(entry, discoveryProjection.id, ServiceDiscovery.class);
        try
        {
            return serviceDiscovery.queryForNames();
        }
        catch ( Exception e )
        {
            ThreadUtils.checkInterrupted(e);
            throw new RpcException(e);
        }
    }

    @ThriftMethod
    public DiscoveryInstance queryForInstance(CuratorProjection projection, DiscoveryProjection discoveryProjection, String name, String id) throws RpcException
    {
        CuratorEntry entry = CuratorEntry.mustGetEntry(connectionManager, projection);
        @SuppressWarnings("unchecked")
        ServiceDiscovery<byte[]> serviceDiscovery = CuratorEntry.mustGetThing(entry, discoveryProjection.id, ServiceDiscovery.class);
        try
        {
            return new DiscoveryInstance(serviceDiscovery.queryForInstance(name, id));
        }
        catch ( Exception e )
        {
            ThreadUtils.checkInterrupted(e);
            throw new RpcException(e);
        }
    }

    @ThriftMethod
    public Collection<DiscoveryInstance> queryForInstances(CuratorProjection projection, DiscoveryProjection discoveryProjection, String name) throws RpcException
    {
        CuratorEntry entry = CuratorEntry.mustGetEntry(connectionManager, projection);
        @SuppressWarnings("unchecked")
        ServiceDiscovery<byte[]> serviceDiscovery = CuratorEntry.mustGetThing(entry, discoveryProjection.id, ServiceDiscovery.class);
        try
        {
            Collection<ServiceInstance<byte[]>> instances = serviceDiscovery.queryForInstances(name);
            Collection<DiscoveryInstance> transformed = Collections2.transform
            (
                instances,
                new Function<ServiceInstance<byte[]>, DiscoveryInstance>()
                {
                    @Override
                    public DiscoveryInstance apply(ServiceInstance<byte[]> instance)
                    {
                        return new DiscoveryInstance(instance);
                    }
                }
            );
            return Lists.newArrayList(transformed);
        }
        catch ( Exception e )
        {
            ThreadUtils.checkInterrupted(e);
            throw new RpcException(e);
        }
    }
}
