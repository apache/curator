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

import org.apache.curator.x.rest.CuratorRestContext;
import org.codehaus.jackson.node.ObjectNode;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/curator/v1/session")
public class SessionResource
{
    private final CuratorRestContext context;

    public SessionResource(@Context CuratorRestContext context)
    {
        this.context = context;
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    public Response newSession() throws Exception
    {
        String sessionId = context.getSessionManager().newSession();
        ObjectNode node = Constants.makeIdNode(context, sessionId);
        return Response.ok(context.getWriter().writeValueAsString(node)).build();
    }

    @DELETE
    @Path("{id}")
    public Response deleteSession(@PathParam("id") String id)
    {
        if ( !context.getSessionManager().deleteSession(id) )
        {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
        return Response.ok().build();
    }

    @GET
    @Path("{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response pingSession(@PathParam("id") String id) throws Exception
    {
        if ( !context.getSessionManager().pingSession(id) )
        {
            return Response.status(Response.Status.NOT_FOUND).build();
        }

        ObjectNode node = context.getMapper().createObjectNode();
        node.put("state", context.getClient().getState().name());
        node.putPOJO("messages", context.drainMessages());

        return Response.ok(context.getWriter().writeValueAsString(node)).build();
    }
}
