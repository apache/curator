package org.apache.curator.x.rpc.idl.discovery;

import com.facebook.swift.service.ThriftMethod;
import com.facebook.swift.service.ThriftService;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.rpc.connections.ConnectionManager;
import org.apache.curator.x.rpc.connections.CuratorEntry;
import org.apache.curator.x.rpc.idl.exceptions.RpcException;
import org.apache.curator.x.rpc.idl.structs.CuratorProjection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collection;

@ThriftService
public class DiscoveryServiceLowLevel
{
    private final Logger log = LoggerFactory.getLogger(getClass());
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
            serviceDiscovery.registerService(null); // TODO
        }
        catch ( Exception e )
        {
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
            serviceDiscovery.updateService(null); // TODO
        }
        catch ( Exception e )
        {
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
            serviceDiscovery.unregisterService(null); // TODO
        }
        catch ( Exception e )
        {
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
            return Collections2.transform
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
        }
        catch ( Exception e )
        {
            throw new RpcException(e);
        }
    }
}
