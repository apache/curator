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
package org.apache.curator.x.rest.api;

import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.curator.x.rest.CuratorRestContext;
import org.apache.curator.x.rest.entities.LockSpec;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.concurrent.TimeUnit;

@Path("/curator/v1/recipes/read-write-lock")
public class ReadWriteLockResource
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final CuratorRestContext context;

    public ReadWriteLockResource(@Context CuratorRestContext context)
    {
        this.context = context;
    }

    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/read")
    public Response acquireReadLock(final LockSpec lockSpec) throws Exception
    {
        return internalLock(lockSpec, false);
    }

    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/write")
    public Response acquireWriteLock(final LockSpec lockSpec) throws Exception
    {
        return internalLock(lockSpec, true);
    }

    private Response internalLock(final LockSpec lockSpec, boolean writeLock) throws Exception
    {
        InterProcessReadWriteLock lock = new InterProcessReadWriteLock(context.getClient(), lockSpec.getPath());
        InterProcessMutex actualLock = writeLock ? lock.writeLock() : lock.readLock();
        if ( !actualLock.acquire(lockSpec.getMaxWaitMs(), TimeUnit.MILLISECONDS) )
        {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).build();
        }

        Closer<InterProcessMutex> closer = new Closer<InterProcessMutex>()
        {
            @Override
            public void close(InterProcessMutex lock)
            {
                if ( lock.isAcquiredInThisProcess() )
                {
                    try
                    {
                        lock.release();
                    }
                    catch ( Exception e )
                    {
                        log.error("Could not release left-over read/write lock for path: " + lockSpec.getPath(), e);
                    }
                }
            }
        };
        String id = context.getSession().addThing(actualLock, closer);
        ObjectNode node = Constants.makeIdNode(context, id);
        return Response.ok(context.getWriter().writeValueAsString(node)).build();
    }

    @DELETE
    @Path("{lock-id}")
    public Response releaseLock(@PathParam("lock-id") String lockId) throws Exception
    {
        InterProcessMutex lock = Constants.deleteThing(context.getSession(), lockId, InterProcessMutex.class);
        lock.release();
        return Response.ok().build();
    }
}
