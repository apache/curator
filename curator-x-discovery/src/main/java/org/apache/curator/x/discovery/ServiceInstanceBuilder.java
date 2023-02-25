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

package org.apache.curator.x.discovery;

import com.google.common.collect.Lists;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Collection;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Builder for service instances
 */
public class ServiceInstanceBuilder<T>
{
    private T payload;
    private String name;
    private String address;
    private Integer port;
    private Integer sslPort;
    private String id;
    private long registrationTimeUTC;
    private ServiceType serviceType = ServiceType.DYNAMIC;
    private UriSpec uriSpec;
    private boolean enabled = true;

    private static final AtomicReference<LocalIpFilter> localIpFilter = new AtomicReference<LocalIpFilter>
    (
        new LocalIpFilter()
        {
            @Override
            public boolean use(NetworkInterface nif, InetAddress adr) throws SocketException
            {
                return (adr != null) && !adr.isLoopbackAddress() && (nif.isPointToPoint() || !adr.isLinkLocalAddress());
            }
        }
    );

    /**
     * Replace the default local ip filter used by {@link #getAllLocalIPs()}
     *
     * @param newLocalIpFilter the new local ip filter
     */
    public static void setLocalIpFilter(LocalIpFilter newLocalIpFilter)
    {
        localIpFilter.set(newLocalIpFilter);
    }

    /**
     * Return the current local ip filter used by {@link #getAllLocalIPs()}
     *
     * @return ip filter
     */
    public static LocalIpFilter getLocalIpFilter()
    {
        return localIpFilter.get();
    }

    ServiceInstanceBuilder()
    {
    }

    /**
     * Return a new instance with the currently set values
     *
     * @return instance
     */
    public ServiceInstance<T> build()
    {
        return new ServiceInstance<T>(name, id, address, port, sslPort, payload, registrationTimeUTC, serviceType, uriSpec, enabled);
    }

    public ServiceInstanceBuilder<T> name(String name)
    {
        this.name = name;
        return this;
    }

    public ServiceInstanceBuilder<T> address(String address)
    {
        this.address = address;
        return this;
    }

    public ServiceInstanceBuilder<T> id(String id)
    {
        this.id = id;
        return this;
    }

    public ServiceInstanceBuilder<T> port(int port)
    {
        this.port = port;
        return this;
    }

    public ServiceInstanceBuilder<T> sslPort(int port)
    {
        this.sslPort = port;
        return this;
    }

    public ServiceInstanceBuilder<T> payload(T payload)
    {
        this.payload = payload;
        return this;
    }

    public ServiceInstanceBuilder<T> serviceType(ServiceType serviceType)
    {
        this.serviceType = serviceType;
        return this;
    }

    public ServiceInstanceBuilder<T> registrationTimeUTC(long registrationTimeUTC)
    {
        this.registrationTimeUTC = registrationTimeUTC;
        return this;
    }

    public ServiceInstanceBuilder<T> uriSpec(UriSpec uriSpec)
    {
        this.uriSpec = uriSpec;
        return this;
    }

    public ServiceInstanceBuilder<T> enabled(boolean enabled)
    {
        this.enabled = enabled;
        return this;
    }

    /**
     * based on http://pastebin.com/5X073pUc
     * <p>
     *
     * Returns all available IP addresses.
     * <p>
     * In error case or if no network connection is established, we return
     * an empty list here.
     * <p>
     * Loopback addresses are excluded - so 127.0.0.1 will not be never
     * returned.
     * <p>
     * The "primary" IP might not be the first one in the returned list.
     *
     * @return  Returns all IP addresses (can be an empty list in error case
     *          or if network connection is missing).
     * @since   0.1.0
     * @throws SocketException errors
     */
    public static Collection<InetAddress> getAllLocalIPs() throws SocketException
    {
        List<InetAddress> listAdr = Lists.newArrayList();
        Enumeration<NetworkInterface> nifs = NetworkInterface.getNetworkInterfaces();
        if (nifs == null) return listAdr;

        while (nifs.hasMoreElements())
        {
            NetworkInterface nif = nifs.nextElement();
            // We ignore subinterfaces - as not yet needed.

            Enumeration<InetAddress> adrs = nif.getInetAddresses();
            while ( adrs.hasMoreElements() )
            {
                InetAddress adr = adrs.nextElement();
                if ( localIpFilter.get().use(nif, adr) )
                {
                    listAdr.add(adr);
                }
            }
        }
        return listAdr;
    }
}
