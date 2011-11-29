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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import java.io.Closeable;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.List;

/**
 * A mechanism to register and query service instances using ZooKeeper
 */
public class ServiceDiscovery<T> implements Closeable
{
    private final CuratorFramework client;
    private final String basePath;
    private final InstanceSerializer<T> serializer;
    private final ServiceInstance<T> thisInstance;
    private final Collection<ServiceCache<T>> caches = Sets.newSetFromMap(Maps.<ServiceCache<T>, Boolean>newConcurrentMap());

    /**
     * @param client the client
     * @param basePath base path to store data
     * @param serializer serializer for instances (e.g. {@link JsonInstanceSerializer})
     */
    public ServiceDiscovery(CuratorFramework client, String basePath, InstanceSerializer<T> serializer)
    {
        this(client, basePath, serializer, null);
    }

    /**
     * @param client the client
     * @param basePath base path to store data
     * @param serializer serializer for instances (e.g. {@link JsonInstanceSerializer})
     * @param thisInstance instance that represents the service that is running. The instance will get auto-registered
     */
    public ServiceDiscovery(CuratorFramework client, String basePath, InstanceSerializer<T> serializer, ServiceInstance<T> thisInstance)
    {
        Preconditions.checkNotNull(client);
        Preconditions.checkNotNull(basePath);
        Preconditions.checkNotNull(serializer);

        this.client = client;
        this.basePath = basePath;
        this.serializer = serializer;
        this.thisInstance = thisInstance;
    }

    /**
     * The discovery must be started before use
     *
     * @throws Exception errors
     */
    public void start() throws Exception
    {
        if ( thisInstance != null )
        {
            registerService(thisInstance);
        }
    }

    @Override
    public void close() throws IOException
    {
        for ( ServiceCache<T> cache : Lists.newArrayList(caches) )
        {
            Closeables.closeQuietly(cache);
        }

        if ( thisInstance != null )
        {
            try
            {
                unregisterService(thisInstance);
            }
            catch ( Exception e )
            {
                client.getZookeeperClient().getLog().error("Could not unregiter this instance", e);
            }
        }
    }

    /**
     * Register/re-register/update a service instance
     *
     * @param service service to add
     * @throws Exception errors
     */
    public void     registerService(ServiceInstance<T> service) throws Exception
    {
        byte[]          bytes = serializer.serialize(service);
        String          path = pathForInstance(service.getName(), service.getId());

        try
        {
            client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path, bytes);
        }
        catch ( KeeperException.NodeExistsException e )
        {
            client.setData().forPath(path, bytes);
        }
    }

    /**
     * Unregister/remove a service instance
     *
     * @param service the service
     * @throws Exception errors
     */
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
    }

    /**
     * Allocate a new service cache for services of the given name
     *
     * @param forName name of the service to cache
     * @return new cache
     */
    public ServiceCache<T> newServiceCache(String forName)
    {
        ServiceCache<T> cache = new ServiceCache<T>(this, forName);
        caches.add(cache);
        return cache;
    }

    /**
     * Return the names of all known services
     *
     * @return list of service names
     * @throws Exception errors
     */
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

    void    cacheClosed(ServiceCache<T> cache)
    {
        caches.remove(cache);
    }

    CuratorFramework getClient()
    {
        return client;
    }

    Collection<ServiceInstance<T>>  queryForInstances(String name, Watcher watcher) throws Exception
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
                    client.create().creatingParentsIfNeeded().forPath(path, new byte[0]);
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

    private String  pathForName(String name) throws UnsupportedEncodingException
    {
        return ZKPaths.makePath(basePath, name);
    }
}
