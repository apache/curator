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

import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import com.google.common.base.MoreObjects;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceType;
import org.apache.curator.x.discovery.UriSpec;

@ThriftStruct
public class DiscoveryInstance
{
    @ThriftField(1)
    public String name;

    @ThriftField(2)
    public String id;

    @ThriftField(3)
    public String address;

    @ThriftField(4)
    public int port;

    @ThriftField(5)
    public int sslPort;

    @ThriftField(6)
    public byte[] payload;

    @ThriftField(7)
    public long registrationTimeUTC;

    @ThriftField(8)
    public DiscoveryInstanceType serviceType;

    @ThriftField(9)
    public String uriSpec;

    public DiscoveryInstance()
    {
    }

    public DiscoveryInstance(ServiceInstance<byte[]> instance)
    {
        if ( instance != null )
        {
            this.name = instance.getName();
            this.id = instance.getId();
            this.address = instance.getAddress();
            this.port = MoreObjects.firstNonNull(instance.getPort(), 0);
            this.sslPort = MoreObjects.firstNonNull(instance.getSslPort(), 0);
            this.payload = instance.getPayload();
            this.registrationTimeUTC = instance.getRegistrationTimeUTC();
            this.serviceType = DiscoveryInstanceType.valueOf(instance.getServiceType().name());
            this.uriSpec = instance.buildUriSpec();
        }
    }

    public DiscoveryInstance(String name, String id, String address, int port, int sslPort, byte[] payload, long registrationTimeUTC, DiscoveryInstanceType serviceType, String uriSpec)
    {
        this.name = name;
        this.id = id;
        this.address = address;
        this.port = port;
        this.sslPort = sslPort;
        this.payload = payload;
        this.registrationTimeUTC = registrationTimeUTC;
        this.serviceType = serviceType;
        this.uriSpec = uriSpec;
    }

    public ServiceInstance<byte[]> toReal()
    {
        return new ServiceInstance<byte[]>(name, id, address, port, sslPort, payload, registrationTimeUTC, ServiceType.valueOf(serviceType.name()), new UriSpec(uriSpec));
    }
}
