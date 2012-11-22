package com.netflix.curator.x.discovery.server.contexts;

import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import com.netflix.curator.x.discovery.ProviderStrategy;
import com.netflix.curator.x.discovery.ServiceDiscovery;
import com.netflix.curator.x.discovery.server.rest.DiscoveryContext;

/**
 * For convenience, a version of {@link DiscoveryContext} that allows a user to
 * specify their own custom type as the payload.
 * 
 */
@Provider
public class GenericDiscoveryContext<T> implements DiscoveryContext<T>, ContextResolver<DiscoveryContext<T>>
{
    private final ServiceDiscovery<T> serviceDiscovery;
    private final ProviderStrategy<T> providerStrategy;
    private final int instanceRefreshMs;
    
    /**
     *  payloadType is wildcard Class<?> it could be Class<T> except when dealing with situations like
     *  Map<String,String> which cannot be referenced explicitly and instead can only be referenced as Map.class.
     */
    private final Class<?> payloadType;

    public GenericDiscoveryContext(ServiceDiscovery<T> serviceDiscovery, ProviderStrategy<T> providerStrategy, int instanceRefreshMs, Class<?> payloadType)
    {
        this.serviceDiscovery = serviceDiscovery;
        this.providerStrategy = providerStrategy;
        this.instanceRefreshMs = instanceRefreshMs;
        this.payloadType = payloadType;
    }

    @Override
    public ProviderStrategy<T> getProviderStrategy()
    {
        return providerStrategy;
    }

    @Override
    public int getInstanceRefreshMs()
    {
        return instanceRefreshMs;
    }

    @Override
    public ServiceDiscovery<T> getServiceDiscovery()
    {
        return serviceDiscovery;
    }

    @SuppressWarnings("unchecked") // because payloadType is Class<?> instead of Class<T> because of generic generics like Map<String,String>
	@Override
    public void marshallJson(ObjectNode node, String fieldName, T payload) throws Exception
    {
        if ( payload == null )
        {
        	payload = (T) payloadType.newInstance();
        }
        
        node.putPOJO(fieldName, payload);
        
    }

    @SuppressWarnings("unchecked") // 
	@Override
    public T unMarshallJson(JsonNode node) throws Exception
    {
        T payload;
        ObjectMapper mapper = new ObjectMapper();
        payload = (T) mapper.readValue(node, payloadType);
        return payload;
    }

    @Override
    public DiscoveryContext<T> getContext(Class<?> type)
    {
        return this;
    }
}
