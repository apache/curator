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

package org.apache.curator.x.rest;

import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.x.rest.system.Connection;
import org.apache.curator.x.rest.system.ConnectionsManager;
import org.apache.curator.x.rest.system.ThingKey;
import org.apache.curator.x.rest.system.ThingType;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ContextResolver;
import java.util.concurrent.Future;

@Path("zookeeper/connection")
public class ConnectionResource
{
    private final ConnectionsManager connectionsManager;

    public ConnectionResource(@Context ContextResolver<ConnectionsManager> contextResolver)
    {
        connectionsManager = contextResolver.getContext(ConnectionsManager.class);
    }

    @POST
    @Path("{id}/connection-state-change")
    public void registerConnectionStateChange(@Suspended final AsyncResponse asyncResponse, @PathParam("id") String id)
    {
        final Connection connection = connectionsManager.get(id);
        if ( connection == null )
        {
            asyncResponse.resume(Response.status(Response.Status.NOT_FOUND).build());
            return;
        }

        Future<?> future = connectionsManager.getExecutorService().submit(new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    ConnectionState state = connection.blockingPopStateChange();
                    asyncResponse.resume(Response.ok(state.name()).build());
                }
                catch ( InterruptedException e )
                {
                    Thread.currentThread().interrupt();
                    asyncResponse.resume(Response.status(Response.Status.SERVICE_UNAVAILABLE).build());
                }
            }
        });
        connection.putThing(new ThingKey<Future>(ThingType.FUTURE), future);
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    public Response getState(@PathParam("id") String id)
    {
        Connection connection = connectionsManager.get(id);
        if ( connection == null )
        {
            return Response.status(Response.Status.NOT_FOUND).build();
        }

        return Response.ok(connection.getClient().getState().name()).build();
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    public String newConnection() throws Exception
    {
        return connectionsManager.newConnection();
    }

    @DELETE
    @Path("{id}")
    public Response closeConnection(@PathParam("id") String id)
    {
        if ( connectionsManager.closeConnection(id) )
        {
            return Response.ok().build();
        }
        return Response.status(Response.Status.NOT_FOUND).build();
    }
}
