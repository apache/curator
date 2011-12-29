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

package com.netflix.curator.x.discovery.rest;

import com.google.common.collect.Lists;
import com.netflix.curator.x.discovery.ServiceInstance;
import com.netflix.curator.x.discovery.ServiceType;
import com.netflix.curator.x.discovery.entity.ServiceInstances;
import com.netflix.curator.x.discovery.entity.ServiceNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * <p>
 * The JAX-RS web service. Due to how most JAX-RS implementations are written, you must
 * create a concrete class that extends this using your payload type. The concrete class should
 * have the base path that you'd like to use.
 * </p>
 *
 * <p>
 * Because the JAX-RS implementation can create a new instance of the resource for every request,
 * your concrete class must use a context resolver to access the DiscoveryContext. Or, if you
 * are using an IoC framework, you can access it that way.
 * </p>
 *
 * <p>
 * Here's a version that has no payload (i.e.
 * a Void payload):
 * </p>
 * <code><pre>
 * &#64;Path("/")
 * public class MyResource extends DiscoveryResource&lt;Void&gt; {
 *     public MyResource(@Context ContextResolver&lt;DiscoveryContext&lt;Void&gt;&gt; resolver) {
 *         // note: this may not work with all JAX-RS implementations
 *         super(resolver.getContext(DiscoveryContext.class));
 *     }
 * }
 * </pre></code>
 */
public abstract class DiscoveryResource<T>
{
    private static final Logger     log = LoggerFactory.getLogger(DiscoveryResource.class);

    private final DiscoveryContext<T> context;

    public DiscoveryResource(DiscoveryContext<T> context)
    {
        this.context = context;
    }

    @PUT
    @Path("v1/service/{name}/{id}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response     putService(ServiceInstance<T> instance, @PathParam("name") String name, @PathParam("id") String id)
    {
        if ( !instance.getId().equals(id) || !instance.getName().equals(name) )
        {
            log.info("Request where path id and/or name doesn't match entity");
            return Response.status(Response.Status.BAD_REQUEST).build();
        }
        
        if ( instance.getServiceType() == ServiceType.DYNAMIC )
        {
            log.info("Service type cannot be dynamic");
            return Response.status(Response.Status.BAD_REQUEST).build();
        }

        try
        {
            context.getServiceDiscovery().registerService(instance);
        }
        catch ( Exception e )
        {
            log.error("Trying to register service", e);
            return Response.serverError().build();
        }

        return Response.status(Response.Status.CREATED).build();
    }

    @DELETE
    @Path("v1/service/{name}/{id}")
    public Response     removeService(@PathParam("name") String name, @PathParam("id") String id)
    {
        try
        {
            ServiceInstance<T> instance = context.getServiceDiscovery().queryForInstance(name, id);
            if ( instance != null )
            {
                //noinspection unchecked
                context.getServiceDiscovery().unregisterService(instance);
            }
        }
        catch ( Exception e )
        {
            log.error("Trying to delete service", e);
            return Response.serverError().build();
        }
        return Response.ok().build();
    }

    @GET
    @Path("{name}/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response     get(@PathParam("name") String name, @PathParam("id") String id)
    {
        try
        {
            ServiceInstance<T> instance = context.getServiceDiscovery().queryForInstance(name, id);
            if ( instance == null )
            {
                return Response.status(Response.Status.NOT_FOUND).build();
            }
            return Response.ok(instance).build();
        }
        catch ( Exception e )
        {
            log.error(String.format("Trying to get instance (%s) from service (%s)", id, name), e);
            return Response.serverError().build();
        }
    }

    @GET
    @Path("v1/service")
    @Produces(MediaType.APPLICATION_JSON)
    public Response     getAllNames()
    {
        try
        {
            List<String> instances = Lists.newArrayList(context.getServiceDiscovery().queryForNames());
            Collections.sort(instances);
            return Response.ok(new ServiceNames(instances)).build();
        }
        catch ( Exception e )
        {
            log.error("Trying to get service names", e);
            return Response.serverError().build();
        }
    }

    @GET
    @Path("v1/service/{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response     getAll(@PathParam("name") String name)
    {
        try
        {
            Collection<ServiceInstance<T>>  instances = context.getServiceDiscovery().queryForInstances(name);
            return Response.ok(new ServiceInstances<T>(instances)).build();
        }
        catch ( Exception e )
        {
            log.error(String.format("Trying to get instances from service (%s)", name), e);
            return Response.serverError().build();
        }
    }

    @GET
    @Path("v1/anyservice/{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response     getAny(@PathParam("name") String name)
    {
        try
        {
            List<ServiceInstance<T>>   instances = Lists.newArrayList(context.getServiceDiscovery().queryForInstances(name));
            Collections.shuffle(instances);
            ServiceInstance<?>         randomInstance = (instances.size() > 0) ? instances.get(0) : null;
            if ( randomInstance == null )
            {
                return Response.status(Response.Status.NOT_FOUND).build();
            }
            return Response.ok(randomInstance).build();
        }
        catch ( Exception e )
        {
            log.error(String.format("Trying to get any instance from service (%s)", name), e);
            return Response.serverError().build();
        }
    }
}
