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
package org.apache.curator.x.discovery.details;

import com.google.common.base.Preconditions;
import org.apache.curator.x.discovery.ServiceType;
import org.apache.curator.x.discovery.UriSpec;
import org.codehaus.jackson.annotate.JsonTypeInfo;
import org.codehaus.jackson.annotate.JsonTypeInfo.Id;
import java.net.URI;
import java.util.Date;

class TestNewServiceInstance<T>
{
    private final String name;
    private final String id;
    private final String address;
    private final Integer port;
    private final Integer sslPort;
    private final T payload;
    private final long registrationTimeUTC;
    private final ServiceType serviceType;
    private final UriSpec uriSpec;
    private final boolean enabled;
    private final String new1;
    private final Long new2;
    private final Date new3;
    private final URI new4;

    public TestNewServiceInstance(String name, String id, String address, Integer port, Integer sslPort, T payload, long registrationTimeUTC, ServiceType serviceType, UriSpec uriSpec, boolean enabled, String new1, Long new2, Date new3, URI new4)
    {
        name = Preconditions.checkNotNull(name, "name cannot be null");
        id = Preconditions.checkNotNull(id, "id cannot be null");

        this.new1 = new1;
        this.new2 = new2;
        this.new3 = new3;
        this.new4 = new4;
        this.serviceType = serviceType;
        this.uriSpec = uriSpec;
        this.name = name;
        this.id = id;
        this.address = address;
        this.port = port;
        this.sslPort = sslPort;
        this.payload = payload;
        this.registrationTimeUTC = registrationTimeUTC;
        this.enabled = enabled;
    }

    /**
     * Inits to default values. Only exists for deserialization
     */
    TestNewServiceInstance()
    {
        this("", "", null, null, null, null, 0, ServiceType.DYNAMIC, null, true, null, null, null, null);
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

    @JsonTypeInfo(use = Id.CLASS, defaultImpl = Object.class)
    public T getPayload()
    {
        return payload;
    }

    public long getRegistrationTimeUTC()
    {
        return registrationTimeUTC;
    }

    public ServiceType getServiceType()
    {
        return serviceType;
    }

    public UriSpec getUriSpec()
    {
        return uriSpec;
    }

    public boolean isEnabled()
    {
        return enabled;
    }

    public String getNew1()
    {
        return new1;
    }

    public Long getNew2()
    {
        return new2;
    }

    public Date getNew3()
    {
        return new3;
    }

    public URI getNew4()
    {
        return new4;
    }
}
