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
import com.netflix.curator.x.discovery.config.DiscoveryContext;
import com.netflix.curator.x.discovery.entities.ServiceInstances;
import com.netflix.curator.x.discovery.entities.ServiceNames;
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

public abstract class DiscoveryResource<T>
{
    private static final Logger     log = LoggerFactory.getLogger(DiscoveryResource.class);

    protected abstract DiscoveryContext<T>   getContext();

    @PUT
    @Path("{name}/{id}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response     putService(ServiceInstance<T> instance, @PathParam("name") String name, @PathParam("id") String id)
    {
        if ( !instance.getId().equals(id) || !instance.getName().equals(name) )
        {
            return Response.status(Response.Status.BAD_REQUEST).build();
        }
        
        ServiceInstance<T>      copy = instance.changeType(ServiceType.STATIC);
        try
        {
            getContext().getServiceDiscovery().registerService(copy);
        }
        catch ( Exception e )
        {
            log.error("Trying to register service", e);
            return Response.serverError().build();
        }

        return Response.status(Response.Status.CREATED).build();
    }

    @DELETE
    @Path("{name}/{id}")
    public Response     removeService(@PathParam("name") String name, @PathParam("id") String id)
    {
        try
        {
            ServiceInstance<T> instance = getContext().getServiceDiscovery().queryForInstance(name, id);
            if ( instance != null )
            {
                //noinspection unchecked
                getContext().getServiceDiscovery().unregisterService(instance);
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
            ServiceInstance<T> instance = getContext().getServiceDiscovery().queryForInstance(name, id);
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
    @Produces(MediaType.APPLICATION_JSON)
    public Response     getAllNames()
    {
        try
        {
            List<String> instances = Lists.newArrayList(getContext().getServiceDiscovery().queryForNames());
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
    @Path("{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response     getAll(@PathParam("name") String name)
    {
        try
        {
            Collection<ServiceInstance<T>>  instances = getContext().getServiceDiscovery().queryForInstances(name);
            return Response.ok(new ServiceInstances<T>(instances)).build();
        }
        catch ( Exception e )
        {
            log.error(String.format("Trying to get instances from service (%s)", name), e);
            return Response.serverError().build();
        }
    }

    @GET
    @Path("any/{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response     getAny(@PathParam("name") String name)
    {
        try
        {
            List<ServiceInstance<T>>   instances = Lists.newArrayList(getContext().getServiceDiscovery().queryForInstances(name));
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
