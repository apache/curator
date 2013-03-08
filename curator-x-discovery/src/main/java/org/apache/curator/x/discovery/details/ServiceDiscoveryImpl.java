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

package org.apache.curator.x.discovery.details;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.utils.ThreadUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.curator.x.discovery.ServiceCache;
import org.apache.curator.x.discovery.ServiceCacheBuilder;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceProvider;
import org.apache.curator.x.discovery.ServiceProviderBuilder;
import org.apache.curator.x.discovery.ServiceType;
import org.apache.curator.x.discovery.strategies.RoundRobinStrategy;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * A mechanism to register and query service instances using ZooKeeper
 */
public class ServiceDiscoveryImpl<T> implements ServiceDiscovery<T>
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final CuratorFramework client;
    private final String basePath;
    private final InstanceSerializer<T> serializer;
    private final Map<String, ServiceInstance<T>> services = Maps.newConcurrentMap();
    private final Collection<ServiceCache<T>> caches = Sets.newSetFromMap(Maps.<ServiceCache<T>, Boolean>newConcurrentMap());
    private final Collection<ServiceProvider<T>> providers = Sets.newSetFromMap(Maps.<ServiceProvider<T>, Boolean>newConcurrentMap());
    private final ConnectionStateListener connectionStateListener = new ConnectionStateListener()
    {
        @Override
        public void stateChanged(CuratorFramework client, ConnectionState newState)
        {
            if ( newState == ConnectionState.RECONNECTED )
            {
                try
                {
                    log.debug("Re-registering due to reconnection");
                    reRegisterServices();
                }
                catch ( Exception e )
                {
                    log.error("Could not re-register instances after reconnection", e);
                }
            }
        }
    };

    /**
     * @param client the client
     * @param basePath base path to store data
     * @param serializer serializer for instances (e.g. {@link JsonInstanceSerializer})
     * @param thisInstance instance that represents the service that is running. The instance will get auto-registered
     */
    public ServiceDiscoveryImpl(CuratorFramework client, String basePath, InstanceSerializer<T> serializer, ServiceInstance<T> thisInstance)
    {
        this.client = Preconditions.checkNotNull(client, "client cannot be null");
        this.basePath = Preconditions.checkNotNull(basePath, "basePath cannot be null");
        this.serializer = Preconditions.checkNotNull(serializer, "serializer cannot be null");
        if ( thisInstance != null )
        {
            services.put(thisInstance.getId(), thisInstance);
        }
    }

    /**
     * The discovery must be started before use
     *
     * @throws Exception errors
     */
    @Override
    public void start() throws Exception
    {
        client.getConnectionStateListenable().addListener(connectionStateListener);
        reRegisterServices();
    }

    @Override
    public void close() throws IOException
    {
        for ( ServiceCache<T> cache : Lists.newArrayList(caches) )
        {
            Closeables.closeQuietly(cache);
        }
        for ( ServiceProvider<T> provider : Lists.newArrayList(providers) )
        {
            Closeables.closeQuietly(provider);
        }

        for ( ServiceInstance<T> service : services.values() )
        {
            try
            {
                unregisterService(service);
            }
            catch ( Exception e )
            {
                log.error("Could not unregister instance: " + service.getName(), e);
            }
        }

        client.getConnectionStateListenable().removeListener(connectionStateListener);
    }

    /**
     * Register/re-register/update a service instance
     *
     * @param service service to add
     * @throws Exception errors
     */
    @Override
    public void registerService(ServiceInstance<T> service) throws Exception
    {
        services.put(service.getId(), service);
        internalRegisterService(service);
    }

    @Override
    public void updateService(ServiceInstance<T> service) throws Exception
    {
        Preconditions.checkArgument(services.containsKey(service.getId()), "Service is not registered: " + service);

        byte[]          bytes = serializer.serialize(service);
        String          path = pathForInstance(service.getName(), service.getId());
        client.setData().forPath(path, bytes);
    }

    @VisibleForTesting
    protected void     internalRegisterService(ServiceInstance<T> service) throws Exception
    {
        byte[]          bytes = serializer.serialize(service);
        String          path = pathForInstance(service.getName(), service.getId());

        final int       MAX_TRIES = 2;
        boolean         isDone = false;
        for ( int i = 0; !isDone && (i < MAX_TRIES); ++i )
        {
            try
            {
                CreateMode      mode = (service.getServiceType() == ServiceType.DYNAMIC) ? CreateMode.EPHEMERAL : CreateMode.PERSISTENT;
                client.create().creatingParentsIfNeeded().withMode(mode).forPath(path, bytes);
                isDone = true;
            }
            catch ( KeeperException.NodeExistsException e )
            {
                client.delete().forPath(path);  // must delete then re-create so that watchers fire
            }
        }
    }

    /**
     * Unregister/remove a service instance
     *
     * @param service the service
     * @throws Exception errors
     */
    @Override
    public void     unregisterService(ServiceInstance<T> service) throws Exception
    {
        String          path = pathForInstance(service.getName(), service.getId());
        try
        {
            client.delete().forPath(path);
        }
        catch ( KeeperException.NoNodeException ignore )
        {
            // ignore
        }
        services.remove(service.getId());
    }

    /**
     * Allocate a new builder. {@link ServiceProviderBuilder#providerStrategy} is set to {@link RoundRobinStrategy}
     * and {@link ServiceProviderBuilder#refreshPaddingMs} is set to 1 second.
     *
     * @return the builder
     */
    @Override
    public ServiceProviderBuilder<T> serviceProviderBuilder()
    {
        return new ServiceProviderBuilderImpl<T>(this)
            .providerStrategy(new RoundRobinStrategy<T>())
            .threadFactory(ThreadUtils.newThreadFactory("ServiceProvider"));
    }

    /**
     * Allocate a new service cache builder. The refresh padding is defaulted to 1 second.
     *
     * @return new cache builder
     */
    @Override
    public ServiceCacheBuilder<T> serviceCacheBuilder()
    {
        return new ServiceCacheBuilderImpl<T>(this)
            .threadFactory(ThreadUtils.newThreadFactory("ServiceCache"));
    }

    /**
     * Return the names of all known services
     *
     * @return list of service names
     * @throws Exception errors
     */
    @Override
    public Collection<String>   queryForNames() throws Exception
    {
        List<String>        names = client.getChildren().forPath(basePath);
        return ImmutableList.copyOf(names);
    }

    /**
     * Return all known instances for the given service
     *
     * @param name name of the service
     * @return list of instances (or an empty list)
     * @throws Exception errors
     */
    @Override
    public Collection<ServiceInstance<T>>  queryForInstances(String name) throws Exception
    {
        return queryForInstances(name, null);
    }

    /**
     * Return a service instance POJO
     *
     * @param name name of the service
     * @param id ID of the instance
     * @return the instance or <code>null</code> if not found
     * @throws Exception errors
     */
    @Override
    public ServiceInstance<T> queryForInstance(String name, String id) throws Exception
    {
        String          path = pathForInstance(name, id);
        try
        {
            byte[]          bytes = client.getData().forPath(path);
            return serializer.deserialize(bytes);
        }
        catch ( KeeperException.NoNodeException ignore )
        {
            // ignore
        }
        return null;
    }

    void    cacheOpened(ServiceCache<T> cache)
    {
        caches.add(cache);
    }

    void    cacheClosed(ServiceCache<T> cache)
    {
        caches.remove(cache);
    }

    void    providerOpened(ServiceProvider<T> provider)
    {
        providers.add(provider);
    }

    void    providerClosed(ServiceProvider<T> cache)
    {
        providers.remove(cache);
    }

    CuratorFramework getClient()
    {
        return client;
    }

    String  pathForName(String name)
    {
        return ZKPaths.makePath(basePath, name);
    }

    InstanceSerializer<T> getSerializer()
    {
        return serializer;
    }

    List<ServiceInstance<T>>  queryForInstances(String name, Watcher watcher) throws Exception
    {
        ImmutableList.Builder<ServiceInstance<T>>   builder = ImmutableList.builder();
        String                  path = pathForName(name);
        List<String>            instanceIds;

        if ( watcher != null )
        {
            instanceIds = getChildrenWatched(path, watcher, true);
        }
        else
        {
            try
            {
                instanceIds = client.getChildren().forPath(path);
            }
            catch ( KeeperException.NoNodeException e )
            {
                instanceIds = Lists.newArrayList();
            }
        }

        for ( String id : instanceIds )
        {
            ServiceInstance<T> instance = queryForInstance(name, id);
            if ( instance != null )
            {
                builder.add(instance);
            }
        }
        return builder.build();
    }

    private List<String> getChildrenWatched(String path, Watcher watcher, boolean recurse) throws Exception
    {
        List<String>    instanceIds;
        try
        {
            instanceIds = client.getChildren().usingWatcher(watcher).forPath(path);
        }
        catch ( KeeperException.NoNodeException e )
        {
            if ( recurse )
            {
                try
                {
                    client.create().creatingParentsIfNeeded().forPath(path);
                }
                catch ( KeeperException.NodeExistsException ignore )
                {
                    // ignore
                }
                instanceIds = getChildrenWatched(path, watcher, false);
            }
            else
            {
                throw e;
            }
        }
        return instanceIds;
    }

    private String  pathForInstance(String name, String id) throws UnsupportedEncodingException
    {
        return ZKPaths.makePath(pathForName(name), id);
    }

    private void reRegisterServices() throws Exception
    {
        for ( ServiceInstance<T> service : services.values() )
        {
            internalRegisterService(service);
        }
    }
}
