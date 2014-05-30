package org.apache.curator.x.rpc.idl.discovery;

import com.facebook.swift.service.ThriftMethod;
import com.facebook.swift.service.ThriftService;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.curator.x.discovery.DownInstancePolicy;
import org.apache.curator.x.discovery.ProviderStrategy;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceProvider;
import org.apache.curator.x.discovery.strategies.RandomStrategy;
import org.apache.curator.x.discovery.strategies.RoundRobinStrategy;
import org.apache.curator.x.discovery.strategies.StickyStrategy;
import org.apache.curator.x.rpc.connections.Closer;
import org.apache.curator.x.rpc.connections.ConnectionManager;
import org.apache.curator.x.rpc.connections.CuratorEntry;
import org.apache.curator.x.rpc.idl.exceptions.RpcException;
import org.apache.curator.x.rpc.idl.structs.CuratorProjection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
}
