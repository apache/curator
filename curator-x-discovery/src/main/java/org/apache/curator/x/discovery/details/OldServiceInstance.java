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

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import org.apache.curator.x.discovery.ServiceType;
import org.apache.curator.x.discovery.UriSpec;

/**
 * POJO that represents a service instance
 */
class OldServiceInstance<T>
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

    /**
     * @param name name of the service
     * @param id id of this instance (must be unique)
     * @param address address of this instance
     * @param port the port for this instance or null
     * @param sslPort the SSL port for this instance or null
     * @param payload the payload for this instance or null
     * @param registrationTimeUTC the time (in UTC) of the registration
     * @param serviceType type of the service
     * @param uriSpec the uri spec or null
     */
    OldServiceInstance(String name, String id, String address, Integer port, Integer sslPort, T payload, long registrationTimeUTC, ServiceType serviceType, UriSpec uriSpec)
    {
        name = Preconditions.checkNotNull(name, "name cannot be null");
        id = Preconditions.checkNotNull(id, "id cannot be null");

        this.serviceType = serviceType;
        this.uriSpec = uriSpec;
        this.name = name;
        this.id = id;
        this.address = address;
        this.port = port;
        this.sslPort = sslPort;
        this.payload = payload;
        this.registrationTimeUTC = registrationTimeUTC;
    }

    OldServiceInstance()
    {
        this("", "", null, null, null, null, 0, ServiceType.DYNAMIC, null);
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

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, defaultImpl = Object.class)
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

    @SuppressWarnings("RedundantIfStatement")
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

        OldServiceInstance that = (OldServiceInstance)o;

        if ( registrationTimeUTC != that.registrationTimeUTC )
        {
            return false;
        }
        if ( address != null ? !address.equals(that.address) : that.address != null )
        {
            return false;
        }
        if ( id != null ? !id.equals(that.id) : that.id != null )
        {
            return false;
        }
        if ( name != null ? !name.equals(that.name) : that.name != null )
        {
            return false;
        }
        if ( payload != null ? !payload.equals(that.payload) : that.payload != null )
        {
            return false;
        }
        if ( port != null ? !port.equals(that.port) : that.port != null )
        {
            return false;
        }
        if ( serviceType != that.serviceType )
        {
            return false;
        }
        if ( sslPort != null ? !sslPort.equals(that.sslPort) : that.sslPort != null )
        {
            return false;
        }
        if ( uriSpec != null ? !uriSpec.equals(that.uriSpec) : that.uriSpec != null )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (id != null ? id.hashCode() : 0);
        result = 31 * result + (address != null ? address.hashCode() : 0);
        result = 31 * result + (port != null ? port.hashCode() : 0);
        result = 31 * result + (sslPort != null ? sslPort.hashCode() : 0);
        result = 31 * result + (payload != null ? payload.hashCode() : 0);
        result = 31 * result + (int)(registrationTimeUTC ^ (registrationTimeUTC >>> 32));
        result = 31 * result + (serviceType != null ? serviceType.hashCode() : 0);
        result = 31 * result + (uriSpec != null ? uriSpec.hashCode() : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return "ServiceInstance{" + "name='" + name + '\'' + ", id='" + id + '\'' + ", address='" + address + '\'' + ", port=" + port + ", sslPort=" + sslPort + ", payload=" + payload + ", registrationTimeUTC=" + registrationTimeUTC + ", serviceType=" + serviceType + ", uriSpec=" + uriSpec + '}';
    }
}
