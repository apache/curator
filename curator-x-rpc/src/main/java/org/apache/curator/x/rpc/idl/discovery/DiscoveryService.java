package org.apache.curator.x.rpc.idl.discovery;

import com.facebook.swift.service.ThriftMethod;
import com.facebook.swift.service.ThriftService;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
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
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

@ThriftService
public class DiscoveryService
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final ConnectionManager connectionManager;

    public DiscoveryService(ConnectionManager connectionManager)
    {
        this.connectionManager = connectionManager;
    }

    @ThriftMethod
    public DiscoveryProjection startDiscovery(CuratorProjection projection, final String basePath, DiscoveryInstance yourInstance) throws RpcException
    {
        try
        {
            CuratorEntry entry = CuratorEntry.mustGetEntry(connectionManager, projection);
            final ServiceDiscovery<byte[]> serviceDiscovery = ServiceDiscoveryBuilder
                .builder(byte[].class)
                .basePath(basePath)
                .client(entry.getClient())
                .thisInstance(null) // TODO
                .build();
            serviceDiscovery.start();

            Closer closer = new Closer()
            {
                @Override
                public void close()
                {
                    try
                    {
                        serviceDiscovery.close();
                    }
                    catch ( IOException e )
                    {
                        log.error("Could not close ServiceDiscovery with basePath: " + basePath, e);
                    }
                }
            };
            String id = entry.addThing(serviceDiscovery, closer);

            return new DiscoveryProjection(id);
        }
        catch ( Exception e )
        {
            throw new RpcException(e);
        }
    }

    @ThriftMethod
    public DiscoveryProviderProjection startProvider(CuratorProjection projection, DiscoveryProjection discoveryProjection, final String serviceName, ProviderStrategyType providerStrategy, int downTimeoutMs, int downErrorThreshold) throws RpcException
    {
        ProviderStrategy<byte[]> strategy = null;
        switch ( providerStrategy )
        {
            default:
            case RANDOM:
            {
                strategy = new RandomStrategy<byte[]>();
                break;
            }

            case STICKY_RANDOM:
            {
                strategy = new StickyStrategy<byte[]>(new RandomStrategy<byte[]>());
                break;
            }

            case STICKY_ROUND_ROBIN:
            {
                strategy = new StickyStrategy<byte[]>(new RoundRobinStrategy<byte[]>());
                break;
            }

            case ROUND_ROBIN:
            {
                strategy = new RoundRobinStrategy<byte[]>();
                break;
            }
        }

        CuratorEntry entry = CuratorEntry.mustGetEntry(connectionManager, projection);
        @SuppressWarnings("unchecked")
        ServiceDiscovery<byte[]> serviceDiscovery = CuratorEntry.mustGetThing(entry, discoveryProjection.id, ServiceDiscovery.class);
        final ServiceProvider<byte[]> serviceProvider = serviceDiscovery
            .serviceProviderBuilder()
            .downInstancePolicy(new DownInstancePolicy(downTimeoutMs, TimeUnit.MILLISECONDS, downErrorThreshold))
            .providerStrategy(strategy)
            .serviceName(serviceName)
            .build();
        try
        {
            serviceProvider.start();
            Closer closer = new Closer()
            {
                @Override
                public void close()
                {
                    try
                    {
                        serviceProvider.close();
                    }
                    catch ( IOException e )
                    {
                        log.error("Could not close ServiceProvider with serviceName: " + serviceName, e);
                    }
                }
            };
            String id = entry.addThing(serviceProvider, closer);
            return new DiscoveryProviderProjection(id);
        }
        catch ( Exception e )
        {
            throw new RpcException(e);
        }
    }

    @ThriftMethod
    public DiscoveryInstance getInstance(CuratorProjection projection, DiscoveryProviderProjection providerProjection) throws RpcException
    {
        CuratorEntry entry = CuratorEntry.mustGetEntry(connectionManager, projection);
        @SuppressWarnings("unchecked")
        ServiceProvider<byte[]> serviceProvider = CuratorEntry.mustGetThing(entry, providerProjection.id, ServiceProvider.class);
        try
        {
            return new DiscoveryInstance(serviceProvider.getInstance());
        }
        catch ( Exception e )
        {
            throw new RpcException(e);
        }
    }

    @ThriftMethod
    public Collection<DiscoveryInstance> getAllInstances(CuratorProjection projection, DiscoveryProviderProjection providerProjection) throws RpcException
    {
        CuratorEntry entry = CuratorEntry.mustGetEntry(connectionManager, projection);
        @SuppressWarnings("unchecked")
        ServiceProvider<byte[]> serviceProvider = CuratorEntry.mustGetThing(entry, providerProjection.id, ServiceProvider.class);
        try
        {
            Collection<ServiceInstance<byte[]>> allInstances = serviceProvider.getAllInstances();
            return Collections2.transform
            (
                allInstances,
                new Function<ServiceInstance<byte[]>, DiscoveryInstance>()
                {
                    @Override
                    public DiscoveryInstance apply(ServiceInstance<byte[]> instance)
                    {
                        return new DiscoveryInstance(instance);
                    }
                });
        }
        catch ( Exception e )
        {
            throw new RpcException(e);
        }
    }

    @ThriftMethod
    public void noteError(CuratorProjection projection, DiscoveryProviderProjection providerProjection, String instanceId) throws RpcException
    {
        CuratorEntry entry = CuratorEntry.mustGetEntry(connectionManager, projection);
        @SuppressWarnings("unchecked")
        ServiceProvider<byte[]> serviceProvider = CuratorEntry.mustGetThing(entry, providerProjection.id, ServiceProvider.class);
        try
        {
            for ( ServiceInstance<byte[]> instance : serviceProvider.getAllInstances() )
            {
                if ( instance.getId().equals(instanceId) )
                {
                    serviceProvider.noteError(instance);
                    break;
                }
            }
        }
        catch ( Exception e )
        {
            throw new RpcException(e);
        }
    }
}
