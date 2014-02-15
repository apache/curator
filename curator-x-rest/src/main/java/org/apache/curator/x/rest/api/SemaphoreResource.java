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

import com.google.common.collect.Lists;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreV2;
import org.apache.curator.framework.recipes.locks.Lease;
import org.apache.curator.x.rest.CuratorRestContext;
import org.apache.curator.x.rest.details.Closer;
import org.apache.curator.x.rest.details.Session;
import org.apache.curator.x.rest.entities.SemaphoreSpec;
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
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Path("/curator/v1/recipes/semaphore/{session-id}")
public class SemaphoreResource
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final CuratorRestContext context;

    private static class LeasesHolder
    {
        final List<Lease> leases;

        private LeasesHolder(Collection<Lease> leases)
        {
            this.leases = Lists.newArrayList(leases);
        }
    }

    public SemaphoreResource(@Context CuratorRestContext context)
    {
        this.context = context;
    }

    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response acquireSemaphore(@PathParam("session-id") String sessionId, final SemaphoreSpec semaphoreSpec) throws Exception
    {
        Session session = Constants.getSession(context, sessionId);
        final InterProcessSemaphoreV2 semaphore = new InterProcessSemaphoreV2(context.getClient(), semaphoreSpec.getPath(), semaphoreSpec.getMaxLeases());
        final Collection<Lease> leases = semaphore.acquire(semaphoreSpec.getAcquireQty(), semaphoreSpec.getMaxWaitMs(), TimeUnit.MILLISECONDS);
        if ( leases == null )
        {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).build();
        }

        Closer<LeasesHolder> closer = new Closer<LeasesHolder>()
        {
            @Override
            public void close(LeasesHolder holder)
            {
                try
                {
                    semaphore.returnAll(holder.leases);
                }
                catch ( Exception e )
                {
                    log.error("Could not release left-over semaphore leases for path: " + semaphoreSpec.getPath(), e);
                }
            }
        };
        String id = session.addThing(new LeasesHolder(leases), closer);
        ObjectNode node = Constants.makeIdNode(context, id);
        return Response.ok(context.getWriter().writeValueAsString(node)).build();
    }

    @DELETE
    @Path("{lease-id}/{release-qty}")
    public Response releaseSemaphore(@PathParam("session-id") String sessionId, @PathParam("lease-id") String leaseId, @PathParam("release-qty") int releaseQty) throws Exception
    {
        Session session = Constants.getSession(context, sessionId);
        LeasesHolder holder = Constants.getThing(session, leaseId, LeasesHolder.class);
        if ( holder.leases.size() < releaseQty )
        {
            return Response.status(Response.Status.NOT_FOUND).build();
        }

        while ( releaseQty-- > 0 )
        {
            Lease lease = holder.leases.remove(0);
            lease.close();
        }
        if ( holder.leases.size() == 0 )
        {
            session.deleteThing(leaseId, LeasesHolder.class);
        }

        return Response.ok().build();
    }
}
