package org.apache.curator.x.rpc.idl.discovery;

import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import org.apache.curator.x.discovery.ServiceInstance;

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
    public Integer port;

    @ThriftField(5)
    public Integer sslPort;

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
        this.name = instance.getName();
        this.id = instance.getId();
        this.address = instance.getAddress();
        this.port = instance.getPort();
        this.sslPort = instance.getSslPort();
        this.payload = instance.getPayload();
        this.registrationTimeUTC = instance.getRegistrationTimeUTC();
        this.serviceType = DiscoveryInstanceType.valueOf(instance.getServiceType().name());
        this.uriSpec = instance.buildUriSpec();
    }

    public DiscoveryInstance(String name, String id, String address, Integer port, Integer sslPort, byte[] payload, long registrationTimeUTC, DiscoveryInstanceType serviceType, String uriSpec)
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
}
