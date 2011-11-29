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
import java.net.InetAddress;
import java.util.Collection;
import java.util.UUID;

/**
 * POJO that represents a service instance
 */
public class ServiceInstance<T>
{
    private final String        name;
    private final String        id;
    private final String        address;
    private final Integer       port;
    private final Integer       sslPort;
    private final T             payload;

    /**
     * Return a new builder. The {@link #address} is set to the ip of the first
     * NIC in the system. The {@link #id} is set to a random UUID.
     *
     * @return builder
     * @throws Exception errors getting the local IP
     */
    public static<T> ServiceInstanceBuilder<T> builder() throws Exception
    {
        String                  address = null;
        Collection<InetAddress> ips = ServiceInstanceBuilder.getAllLocalIPs();
        if ( ips.size() > 0 )
        {
            address = ips.iterator().next().getHostAddress();   // default to the first address
        }
        String                  id = UUID.randomUUID().toString();
        return new ServiceInstanceBuilder<T>().address(address).id(id);
    }

    /**
     * @param name name of the service
     * @param id id of this instance (must be unique)
     * @param address address of this instance
     * @param port the port for this instance or null
     * @param sslPort the SSL port for this instance or null
     * @param payload the payload for this instance or null
     */
    public ServiceInstance(String name, String id, String address, Integer port, Integer sslPort, T payload)
    {
        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(id);

        this.name = name;
        this.id = id;
        this.address = address;
        this.port = port;
        this.sslPort = sslPort;
        this.payload = payload;
    }

    /**
     * Inits to default values. Only exists for deserialization
     */
    public ServiceInstance()
    {
        this("", "", null, null, null, null);
    }

    public String getName()
    {
        return name;
    }

    public String getId()
    {
        return id;
    }

    public String getAddress()
    {
        return address;
    }

    public Integer getPort()
    {
        return port;
    }

    public Integer getSslPort()
    {
        return sslPort;
    }

    public T getPayload()
    {
        return payload;
    }

    @SuppressWarnings({"RedundantIfStatement"})
    @Override
    public boolean equals(Object o)
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        ServiceInstance that = (ServiceInstance)o;

        if ( !id.equals(that.id) )
        {
            return false;
        }
        if ( !name.equals(that.name) )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = name.hashCode();
        result = 31 * result + id.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return "ServiceInstance{" +
            "name='" + name + '\'' +
            ", id='" + id + '\'' +
            ", address='" + address + '\'' +
            ", port=" + port +
            ", sslPort=" + sslPort +
            ", payload=" + payload +
            '}';
    }
}
