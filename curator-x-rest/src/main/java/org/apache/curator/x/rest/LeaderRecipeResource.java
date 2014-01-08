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

import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.curator.x.rest.entity.LockRequestEntity;
import org.apache.curator.x.rest.system.Connection;
import org.apache.curator.x.rest.system.ConnectionsManager;
import org.apache.curator.x.rest.system.ThingKey;
import org.apache.curator.x.rest.system.ThingType;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
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
import java.util.concurrent.TimeUnit;

@Path("zookeeper/recipes/leader/{connectionId}")
public class LeaderRecipeResource
{
    private final ConnectionsManager connectionsManager;

    public LeaderRecipeResource(@Context ContextResolver<ConnectionsManager> contextResolver)
    {
        connectionsManager = contextResolver.getContext(ConnectionsManager.class);
    }

    @POST
    @Path("{path:.*}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response reEntrantLockAllocate(@PathParam("connectionId") String connectionId, @PathParam("path") String path) throws Exception
    {
        Connection connection = connectionsManager.get(connectionId);
        if ( connection == null )
        {
            return Response.status(Response.Status.NOT_FOUND).build();
        }

        InterProcessSemaphoreMutex mutex = new InterProcessSemaphoreMutex(connection.getClient(), path);
        ThingKey<InterProcessSemaphoreMutex> key = new ThingKey<InterProcessSemaphoreMutex>(ThingType.MUTEX);
        connection.putThing(key, mutex);

        return Response.ok(key.getId()).build();
    }

    @DELETE
    @Path("{id}")
    public Response reEntrantLockDelete(@PathParam("connectionId") String connectionId, @PathParam("id") String lockId) throws Exception
    {
        Connection connection = connectionsManager.get(connectionId);
        if ( connection == null )
        {
            return Response.status(Response.Status.NOT_FOUND).build();
        }

        InterProcessSemaphoreMutex mutex = connection.removeThing(new ThingKey<InterProcessSemaphoreMutex>(lockId, ThingType.MUTEX));
        if ( mutex == null )
        {
            return Response.status(Response.Status.NOT_FOUND).build();
        }

        if ( mutex.isAcquiredInThisProcess() )
        {
            mutex.release();
        }

        return Response.ok().build();
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public void reEntrantLockAcquire(@Suspended final AsyncResponse asyncResponse, @PathParam("connectionId") String connectionId, final LockRequestEntity lockRequest) throws Exception
    {
        Connection connection = connectionsManager.get(connectionId);
        if ( connection == null )
        {
            asyncResponse.resume(Response.status(Response.Status.NOT_FOUND).build());
            return;
        }

        final InterProcessSemaphoreMutex mutex = connection.getThing(new ThingKey<InterProcessSemaphoreMutex>(lockRequest.getLockId(), ThingType.MUTEX));
        if ( mutex == null )
        {
            asyncResponse.resume(Response.status(Response.Status.NOT_FOUND).build());
            return;
        }

        connectionsManager.getExecutorService().submit
        (
            new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        boolean success = mutex.acquire(lockRequest.getMaxWaitMs(), TimeUnit.MILLISECONDS);
                        asyncResponse.resume(Response.status(success ? Response.Status.OK : Response.Status.REQUEST_TIMEOUT).build());
                    }
                    catch ( Exception e )
                    {
                        asyncResponse.resume(e);
                    }
                }
            }
        );
    }
}
